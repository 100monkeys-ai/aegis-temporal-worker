/**
 * Temporal Workflows
 *
 * We export the Generic Interpreter Workflow which handles all
 * AEGIS workflow definitions dynamically.
 */

import { aegis_workflow } from "./aegis-workflow.js";
import { aegis_schedule_fire } from "./schedule-fire.js";

// Export with the specific name that the Rust client invokes
const workflows = {
  "aegis-workflow": aegis_workflow,
  "aegis-schedule-fire": aegis_schedule_fire,
};

export default workflows;

// Individual exports for discovery if needed (though default export covers it)
export { aegis_workflow, aegis_schedule_fire };
