import { proxyActivities, setHandler, defineSignal, condition, workflowInfo } from '@temporalio/workflow';
import Handlebars from 'handlebars';
import type {
    WorkflowResult,
    Blackboard,
    WorkflowState,
    TransitionRule,
    JudgeConfig,
} from '../types.js';
import * as activities from '../activities/index.js';

// Proxy activities
const {
    executeAgentActivity,
    executeSystemCommandActivity,
    validateOutputActivity,
    executeParallelAgentsActivity,
    storeTrajectoryPatternActivity,
    fetchWorkflowDefinition,
    publishEventActivity,
    executeContainerRunActivity,
    executeParallelContainerRunActivity,
} = proxyActivities<typeof activities>({
    startToCloseTimeout: '10 minutes',
    retry: {
        maximumAttempts: 3,
    },
});

// Register Handlebars helpers (idempotent if registered multiple times in sandbox)
Handlebars.registerHelper('length', (value: any) => {
    if (Array.isArray(value)) return value.length;
    if (typeof value === 'string') return value.length;
    if (typeof value === 'object' && value !== null) return Object.keys(value).length;
    return 0;
});
Handlebars.registerHelper('upper', (str: string) => (str || '').toUpperCase());
Handlebars.registerHelper('lower', (str: string) => (str || '').toLowerCase());
Handlebars.registerHelper('trim', (str: string) => (str || '').trim());

interface GenericWorkflowInput {
    workflow_id: string;
    input: Record<string, any>;
    blackboard?: Record<string, any>;
}

/**
 * AEGIS Generic Interpreter Workflow
 * 
 * This workflow acts as an interpreter for AEGIS workflow definitions.
 * Instead of compiling definitions to creating TS code, it fetches the definition
 * at runtime and executes it step-by-step.
 */
export async function aegis_workflow(args: GenericWorkflowInput): Promise<WorkflowResult> {
    const { workflow_id, input, blackboard: blackboardOverrides } = args;
    const info = workflowInfo();
    const executionId = info.workflowId; // In AEGIS, Temporal workflowId is the Execution UUID
    let temporalSequenceNumber = 1;

    let workflowId: string | undefined = workflow_id;

    // Register the humanInput signal at workflow root — MUST be before any await/activity
    // to satisfy Temporal's determinism requirements. A single signal registration covers
    // all Human states in the workflow. clearResponse() is called after each Human state
    // consumes the value so successive Human gates wait for distinct signals.
    const humanInputSignal = defineSignal<[string]>('humanInput');
    let humanResponse: string | null = null;
    setHandler(humanInputSignal, (response: string) => { humanResponse = response; });

    const humanSignal = {
        getResponse: () => humanResponse,
        clearResponse: () => { humanResponse = null; },
    };

    const emit = async (eventType: string, extra: any = {}) => {
        await publishEventActivity({
            event_type: eventType,
            execution_id: executionId,
            temporal_sequence_number: temporalSequenceNumber++,
            workflow_id: workflowId,
            timestamp: new Date().toISOString(),
            ...extra
        });
    };

    await emit('WorkflowExecutionStarted');

    // 1. Fetch Definition
    const definition = await fetchWorkflowDefinition(workflow_id);
    workflowId = definition.workflow_id;

    // 2. Initialize Blackboard
    const blackboard: Blackboard = {
        ...definition.context,
        ...(blackboardOverrides ?? {}),
        workflow: {
            name: definition.name,
            version: definition.version,
            ...input,
        },
    };

    // 3. Execution Loop
    let currentState: string | null = definition.initial_state;
    let iterationCount = 0;
    const maxIterations = 1000;

    while (currentState !== null && iterationCount < maxIterations) {
        iterationCount++;
        const state = definition.states[currentState];

        if (!state) {
            const err = `State "${currentState}" not found in definition`;
            await emit('WorkflowExecutionFailed', { error: err });
            throw new Error(err);
        }

        try {
            await emit('WorkflowStateEntered', { state_name: currentState });

            // Execute State
            const stateOutput = await executeState(state, currentState, blackboard, emit, executionId, humanSignal);

            await emit('WorkflowStateExited', { state_name: currentState, output: stateOutput });

            // Update Blackboard
            blackboard[currentState] = stateOutput;

            // Check Terminal
            if (!state.transitions || state.transitions.length === 0) {
                await emit('WorkflowExecutionCompleted', { final_blackboard: blackboard });
                return {
                    status: 'completed',
                    output: stateOutput,
                    iterations: iterationCount,
                    final_state: currentState,
                    blackboard,
                };
            }

            // Transition
            currentState = await evaluateTransitions(state.transitions, stateOutput, blackboard);

            if (currentState === null) {
                await emit('WorkflowExecutionCompleted', { final_blackboard: blackboard });
                return {
                    status: 'completed',
                    output: stateOutput,
                    iterations: iterationCount,
                    final_state: currentState ?? undefined,
                    blackboard,
                };
            }

        } catch (error) {
            const errMsg = error instanceof Error ? error.message : String(error);
            await emit('WorkflowExecutionFailed', { error: errMsg, final_blackboard: blackboard });
            return {
                status: 'failed',
                error: errMsg,
                iterations: iterationCount,
                final_state: currentState ?? undefined,
                blackboard,
            };
        }
    }

    const err = 'Max iterations exceeded';
    await emit('WorkflowExecutionFailed', { error: err, final_blackboard: blackboard });
    return {
        status: 'failed',
        error: err,
        iterations: iterationCount,
        final_state: currentState ?? undefined,
        blackboard,
    };
}

// ----------------------------------------------------------------------------
// Helper Functions (Same logic as workflow-generator.ts but adapted for static context)
// ----------------------------------------------------------------------------

async function executeState(
    state: WorkflowState,
    stateName: string,
    blackboard: Blackboard,
    emit: (eventType: string, extra?: any) => Promise<void>,
    executionId: string,
    humanSignal: { getResponse: () => string | null; clearResponse: () => void }
): Promise<any> {
    switch (state.kind) {
        case 'Agent':
            if (!state.agent || !state.input) throw new Error("Invalid Agent State");

            let iteration = 1;
            let currentInput = renderTemplate(state.input, blackboard);
            // Iteration bound: state config > workflow-level context > default of 10
            const maxIterations: number = state.max_iterations ?? 10;

            // Judge agents come from the state YAML declaration (ADR-016/017).
            // Falling back to a blackboard key is an anti-pattern and is explicitly removed.
            const judges: JudgeConfig[] = state.judges || [];

            // Trajectory accumulator for Cortex (ADR-049 Pillar 2).
            // tool_name = the agent id acting as the "tool"; arguments_json = serialized output.
            const trajectorySteps: Array<{ tool_name: string; arguments_json: string }> = [];
            let lastOutput = null;

            // ADR-049 Pillar 1 — Pre-execution semantic validation
            // Run the rendered prompt through the pre-execution validator *before* spinning
            // up the agent container.  A score that results in binary_valid === false
            // short-circuits the entire state with a graceful failure so the Workflow can
            // route to a fallback state rather than burning a full iteration budget on a
            // plan the validator already knows will fail.
            if (state.pre_execution_validator) {
                const preValResult = await validateOutputActivity({
                    output: currentInput,
                    task: `Pre-execution plan validation for workflow state: ${stateName}`,
                    judges: [{ agent_id: state.pre_execution_validator, weight: 1.0 }],
                    context_json: JSON.stringify({ execution_id: executionId, state_name: stateName }),
                });
                if (preValResult.binary_valid === false) {
                    await emit('WorkflowIterationFailed', {
                        iteration_number: 0,
                        error: `Pre-execution validator rejected plan: ${preValResult.reasoning}`,
                    });
                    return {
                        status: 'failed' as const,
                        error: `Pre-execution validator (${state.pre_execution_validator}) rejected plan: ${preValResult.reasoning}`,
                        pre_validation_score: preValResult.score,
                    };
                }
            }

            while (iteration <= maxIterations) {
                await emit('WorkflowIterationStarted', { iteration_number: iteration });

                try {
                    const result = await executeAgentActivity({
                        agentId: state.agent,
                        input: currentInput,
                        context: blackboard,
                        workflowExecutionId: executionId,
                    });

                    lastOutput = result;

                    if (result.status !== 'completed') {
                        // Throw here — the catch block below emits WorkflowIterationFailed
                        throw new Error(`Agent execution failed: ${result.error}`);
                    }

                    await emit('WorkflowIterationCompleted', {
                        iteration_number: iteration,
                        output: result.output ?? '',
                    });

                    if (judges.length === 0) {
                        // No judges configured — treat first successful execution as valid.
                        break;
                    }

                    const validationResult = await validateOutputActivity({
                        output: result.output,
                        task: currentInput,
                        judges,
                        consensus_strategy: state.consensus?.strategy,
                        consensus_threshold: state.consensus?.threshold,
                        // Pass execution context so Rust can link judge child executions
                        context_json: JSON.stringify({ execution_id: executionId }),
                    });

                    const iterScore: number = validationResult.score ?? 0;
                    trajectorySteps.push({
                        tool_name: state.agent,
                        arguments_json: JSON.stringify({ output: result.output, score: iterScore }),
                    });

                    if (validationResult.binary_valid === true) {
                        // Successful iteration — commit trajectory to Cortex.
                        await storeTrajectoryPatternActivity({
                            taskSignature: `${stateName}:${executionId}`,
                            steps: trajectorySteps,
                            successScore: iterScore,
                        });
                        break;
                    }

                    if (iteration >= maxIterations) {
                        // Exhausted iterations without passing validation.
                        break;
                    }

                    await emit('RefinementApplied', {
                        iteration_number: iteration,
                        code_diff: validationResult.reasoning,
                        agent_id: state.agent,
                    });

                    currentInput = currentInput + `\n\nValidation failed with score ${iterScore}.\nReasoning: ${validationResult.reasoning}\nPlease refine your response.`;
                    iteration++;
                } catch (error) {
                    const errMsg = error instanceof Error ? error.message : String(error);
                    await emit('WorkflowIterationFailed', {
                        iteration_number: iteration,
                        error: errMsg
                    });
                    throw error;
                }
            }

            return lastOutput;

        case 'System':
            if (!state.command) throw new Error("Invalid System State");
            const env: Record<string, string> = {};
            if (state.env) {
                for (const [k, v] of Object.entries(state.env)) {
                    env[k] = renderTemplate(String(v), blackboard);
                }
            }
            return await executeSystemCommandActivity({
                command: renderTemplate(state.command, blackboard),
                env,
                workdir: state.workdir,
                timeout: state.timeout ? parseTimeout(state.timeout) : undefined,
            });

        case 'Human':
            if (!state.prompt) throw new Error("Invalid Human State");

            // Clear any previous signal response so this Human state receives a fresh one.
            humanSignal.clearResponse();

            await emit('HumanInputRequested', { prompt: state.prompt, default_response: state.default_response });

            // Signal already registered at aegis_workflow() root — just wait for it.
            const timeout = state.timeout ? parseTimeout(state.timeout) : 3600;
            await condition(() => humanSignal.getResponse() !== null, timeout * 1000);

            const hr = humanSignal.getResponse();
            humanSignal.clearResponse();

            if (hr === null) {
                if (state.default_response) return { response: state.default_response, timeout: true };
                throw new Error("Human input timeout");
            }
            return { response: hr, timeout: false };

        case 'ParallelAgents':
            if (!state.agents || !state.consensus) throw new Error("Invalid ParallelAgents State");
            const agentConfigs = state.agents.map(a => ({
                agent: a.agent,
                input: renderTemplate(a.input, blackboard),
                weight: a.weight
            }));
            return await executeParallelAgentsActivity({
                agents: agentConfigs,
                judges: state.judges_for_parallel,
                consensus: {
                    strategy: state.consensus.strategy,
                    threshold: state.consensus.threshold ?? 0.7
                }
            });

        case 'ContainerRun': {
            if (!state.container_run_image || !state.container_run_command) {
                throw new Error(`Invalid ContainerRun state '${stateName}': missing image or command`);
            }

            const crEnv: Record<string, string> = {};
            if (state.container_run_env) {
                for (const [k, v] of Object.entries(state.container_run_env)) {
                    crEnv[k] = renderTemplate(String(v), blackboard);
                }
            }

            await emit('ContainerRunStarted', {
                state_name: stateName,
                name: state.container_run_name,
                image: state.container_run_image,
            });

            const crResult = await executeContainerRunActivity({
                execution_id: executionId,
                state_name: stateName,
                name: state.container_run_name ?? stateName,
                image: state.container_run_image,
                image_pull_policy: state.container_run_image_pull_policy,
                command: state.container_run_command,
                env: crEnv,
                workdir: state.container_run_workdir,
                volumes: state.container_run_volumes ?? [],
                resources: state.container_run_resources,
                registry_credentials: state.container_run_registry_credentials,
                shell: state.container_run_shell ?? false,
                max_attempts: state.container_run_retry?.max_attempts ?? 1,
            });

            if (crResult.exit_code === 0) {
                await emit('ContainerRunCompleted', {
                    state_name: stateName,
                    exit_code: crResult.exit_code,
                    duration_ms: crResult.duration_ms,
                    attempts: crResult.attempts,
                });
            } else {
                await emit('ContainerRunFailed', {
                    state_name: stateName,
                    exit_code: crResult.exit_code,
                    stderr: crResult.stderr,
                    duration_ms: crResult.duration_ms,
                    attempts: crResult.attempts,
                });
            }

            return {
                status: crResult.exit_code === 0 ? 'success' : 'failed',
                output: {
                    exit_code: crResult.exit_code,
                    stdout: crResult.stdout,
                    stderr: crResult.stderr,
                    duration_ms: crResult.duration_ms,
                    attempts: crResult.attempts,
                },
                exit_code: crResult.exit_code,
                stdout: crResult.stdout,
                stderr: crResult.stderr,
                duration_ms: crResult.duration_ms,
                attempts: crResult.attempts,
            };
        }

        case 'ParallelContainerRun': {
            if (!state.parallel_container_steps || state.parallel_container_steps.length === 0) {
                throw new Error(`Invalid ParallelContainerRun state '${stateName}': no steps defined`);
            }

            const renderedSteps = state.parallel_container_steps.map((step) => ({
                ...step,
                env: step.env
                    ? Object.fromEntries(
                          Object.entries(step.env).map(([k, v]) => [k, renderTemplate(String(v), blackboard)])
                      )
                    : undefined,
            }));

            await emit('ContainerRunStarted', {
                state_name: stateName,
                step_count: renderedSteps.length,
                completion: state.parallel_container_completion ?? 'all_succeed',
            });

            const pcrResult = await executeParallelContainerRunActivity({
                execution_id: executionId,
                state_name: stateName,
                steps: renderedSteps,
                completion: state.parallel_container_completion ?? 'all_succeed',
            });

            const byStep = Object.fromEntries(
                pcrResult.results.map((r) => [r.name, {
                    exit_code: r.exit_code,
                    stdout: r.stdout,
                    stderr: r.stderr,
                    duration_ms: r.duration_ms,
                }])
            );

            if (pcrResult.overall_success) {
                await emit('ContainerRunCompleted', {
                    state_name: stateName,
                    overall_success: pcrResult.overall_success,
                    completion: pcrResult.completion,
                    succeeded: pcrResult.succeeded,
                    failed: pcrResult.failed,
                    step_results: pcrResult.results.map((r) => ({
                        name: r.name,
                        exit_code: r.exit_code,
                        duration_ms: r.duration_ms,
                    })),
                });
            } else {
                await emit('ContainerRunFailed', {
                    state_name: stateName,
                    overall_success: pcrResult.overall_success,
                    completion: pcrResult.completion,
                    succeeded: pcrResult.succeeded,
                    failed: pcrResult.failed,
                    step_results: pcrResult.results.map((r) => ({
                        name: r.name,
                        exit_code: r.exit_code,
                        stderr: r.stderr,
                    })),
                });
            }

            return {
                status: pcrResult.overall_success ? 'success' : 'failed',
                overall_success: pcrResult.overall_success,
                completion: pcrResult.completion,
                succeeded: pcrResult.succeeded,
                failed: pcrResult.failed,
                output: byStep,
                results: pcrResult.results,
            };
        }

        default:
            throw new Error(`Unknown state kind: ${state.kind}`);
    }
}

async function evaluateTransitions(
    transitions: TransitionRule[],
    stateOutput: any,
    blackboard: Blackboard
): Promise<string | null> {
    for (const t of transitions) {
        if (await evaluateCondition(t, stateOutput, blackboard)) {
            return t.target;
        }
    }
    return null;
}

async function evaluateCondition(t: TransitionRule, output: any, bb: Blackboard): Promise<boolean> {
    const resolvedExitCode =
        typeof output?.exit_code === 'number'
            ? output.exit_code
            : typeof output?.output?.exit_code === 'number'
            ? output.output.exit_code
            : undefined;

    const resolvedStatus = output?.status;

    switch (t.condition) {
        case 'always': return true;
        case 'on_success':
            return resolvedStatus === 'completed' ||
                resolvedStatus === 'success' ||
                resolvedExitCode === 0 ||
                output?.overall_success === true;
        case 'on_failure':
            return resolvedStatus === 'failed' ||
                resolvedStatus === 'error' ||
                (typeof resolvedExitCode === 'number' && resolvedExitCode !== 0) ||
                output?.overall_success === false;
        case 'exit_code_zero':
            return resolvedExitCode === 0;
        case 'exit_code_non_zero':
            return typeof resolvedExitCode === 'number' && resolvedExitCode !== 0;
        case 'exit_code':
            return typeof resolvedExitCode === 'number' && resolvedExitCode === t.exit_code;
        case 'score_above': return (output?.score || output?.final_score || 0) > (t.threshold || 0);
        case 'score_below': return (output?.score || output?.final_score || 0) < (t.threshold || 1);
        case 'score_between':
            const score = output?.score || output?.final_score || 0;
            return score >= (t.min || 0) && score <= (t.max || 1);
        case 'confidence_above':
            return (output?.confidence || 0) > (t.threshold || 0);
        case 'score_and_confidence_above':
            return (output?.score || output?.final_score || 0) > (t.threshold || 0) &&
                (output?.confidence || 0) > (t.threshold || 0);
        case 'consensus':
            const consensusScore = output?.consensus?.score ?? output?.final_score ?? 0;
            const consensusConfidence = output?.consensus?.confidence ?? output?.confidence ?? 0;
            return consensusScore >= (t.threshold || 0) && consensusConfidence >= (t.agreement || 0);
        case 'all_approved':
            return Array.isArray(output?.individual_scores) &&
                output.individual_scores.length > 0 &&
                output.individual_scores.every((s: number) => s >= (t.threshold || 0.8));
        case 'any_rejected':
            return Array.isArray(output?.individual_scores) &&
                output.individual_scores.some((s: number) => s < (t.threshold || 0.8));
        case 'input_equals': return output?.response === t.value;
        case 'input_equals_yes': return ['yes', 'y', '1'].includes(String(output?.response).toLowerCase());
        case 'input_equals_no': return ['no', 'n', '0'].includes(String(output?.response).toLowerCase());
        case 'custom':
            if (!t.expression) return false;
            try {
                const tmpl = `{{#if ${t.expression}}}true{{else}}false{{/if}}`;
                return renderTemplate(tmpl, { ...bb, state_output: output }).trim() === 'true';
            } catch {
                // If custom handlebar evaluation throws, we assume false to prevent FSM crash
                return false;
            }
        default: return false;
    }
}

function renderTemplate(tmpl: string, ctx: any): string {
    return Handlebars.compile(tmpl)(ctx);
}

function parseTimeout(str: string): number {
    const m = str.match(/^(\d+)([smh])$/);
    if (!m) return 60;
    const v = parseInt(m[1]);
    const u = m[2];
    if (u === 'm') return v * 60;
    if (u === 'h') return v * 3600;
    return v;
}
