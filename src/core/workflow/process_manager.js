const processDependency = require("./process");
const { Timer } = require("./timer");
const workflowDependency = require("./workflow");

module.exports.createProcessByWorkflowName = async function (workflow_name, actor_data, initial_bag = {}) {
  const workflow = await workflowDependency.Workflow.fetchWorkflowByName(workflow_name);
  if (workflow) {
    return await workflow.createProcess(actor_data, initial_bag);
  }
  return undefined;
};

module.exports.runProcess = async function (process_id, actor_data, external_input) {
  const process = await processDependency.Process.fetch(process_id);
  if (process) {
    return await process.run(actor_data, external_input);
  }
  return undefined;
};

module.exports.continueProcess = async function (process_id, result_data, expected_step_number) {
  const process = await processDependency.Process.fetch(process_id);
  const next_step_number = processDependency.Process.calculateNextStep(process.state.step_number);
  if (process && next_step_number === expected_step_number) {
    return await process.continue(result_data, process.state.actor_data);
  } else {
    return undefined;
  }
};

module.exports.abortProcess = async function (process_ids) {
  const abort_promises = process_ids.map(async (process_id) => {
    const process = await processDependency.Process.fetch(process_id);
    if (process) {
      emitter.emit("PROCESS.TIMER.CREATING", `  CREATING PROCESS TIMER ON PID [${process_id}]`, { process_id: process_id });
      let timer = new Timer("Process", process_id, Timer.timeoutFromNow(0), { actor_data: process.state.result.actor_data, reason: "ABORT" });
      await timer.save();
      emitter.emit("PROCESS.TIMER.NEW", `  PROCESS TIMER ON PID [${process_id}] TIMER [${timer.id}]`, {
        process_id: process_id,
        timer_id: timer.id,
      });
    } else {
      throw new Error(`Process not found ${process_id}`);
    }
  });
  return Promise.allSettled(abort_promises);
};

module.exports.notifyCompletedActivityManager = async function (
  process_id,
  { actor_data, activities },
  expected_step_number
) {
  const process = await processDependency.Process.fetch(process_id);
  const next_step_number = processDependency.Process.calculateNextStep(process.state.step_number);
  let result;
  if (process) {
    let process_promise;
    if (next_step_number === expected_step_number) {
      process_promise = process.run(actor_data, { activities: activities });
    } else {
      process_promise = Promise.resolve(process);
    }
    result = {
      process_promise,
    };
  }
  return result;
};
