const obju = require("../../utils/object");
const { prepare } = require("../../utils/input");
const { ProcessStatus } = require("../process_state");
const { Validator } = require("../../validators");
const process_manager = require("../process_manager");
const { SystemTaskNode } = require("./systemTask");

class StartProcessSystemTaskNode extends SystemTaskNode {
  static get rules() {
    const parameters_rules = {
      parameters_has_workflow_name: [obju.hasField, "workflow_name"],
      parameters_workflow_name_has_valid_type: [obju.isFieldTypeIn, "workflow_name", ["string", "object"]],
      parameters_has_actor_data: [obju.hasField, "actor_data"],
      parameters_actor_data_has_valid_type: [obju.isFieldOfType, "actor_data", "object"],
    };

    return {
      ...super.rules,
      parameters_nested_validations: [new Validator(parameters_rules), "parameters"],
    };
  }

  validate() {
    return StartProcessSystemTaskNode.validate(this._spec);
  }

  _preProcessing({ bag, input, actor_data, environment, parameters, process_id }) {
    const context = {
      bag,
      result: input,
      actor_data,
      environment,
      parameters,
    };

    const prepared_input = super._preProcessing({
      bag,
      input,
      actor_data,
      environment,
      parameters,
    });
    const prepared_workflow_name = prepare(this._spec.parameters.workflow_name, context);
    const prepared_actor_data = prepare(this._spec.parameters.actor_data, context);

    return {
      workflow_name: prepared_workflow_name,
      input: prepared_input,
      actor_data: { ...prepared_actor_data, ...{ parentProcessData: { id: process_id } } },
    };
  }

  async run({
    bag = {},
    input = {},
    external_input = {},
    actor_data = {},
    environment = {},
    process_id = null,
    parameters = {},
  }) {
    emitter.emit("NODE.RUN_BEGIN", `NODE RUN START PROCESS BEGUN PID [${process_id}]`, { process_id });
    const hrt_run_start = process.hrtime();
    try {
      const execution_data = this._preProcessing({ bag, input, actor_data, environment, parameters, process_id });
      const [result, status] = await this._run(execution_data);

      const hrt_run_interval = process.hrtime(hrt_run_start);
      const time_elapsed = Math.ceil(hrt_run_interval[0] * 1000 + hrt_run_interval[1] / 1000000);

      return {
        node_id: this.id,
        bag: this._setBag(bag, result),
        external_input: external_input,
        result: result,
        error: null,
        status: status,
        next_node_id: this.next(result),
        time_elapsed: time_elapsed,
      };
    } catch (err) {
      const hrt_run_interval = process.hrtime(hrt_run_start);
      const time_elapsed = Math.ceil(hrt_run_interval[0] * 1000 + hrt_run_interval[1] / 1000000);
      return this._processError(err, { bag, external_input, time_elapsed });
    }
  }

  async _run(execution_data) {
    const process = await process_manager.createProcessByWorkflowName(
      execution_data.workflow_name,
      execution_data.actor_data,
      execution_data.input
    );
    process_manager.runProcess(process.id, execution_data.actor_data);

    if (!process.id) {
      emitter.emit(
        "NODE.RUN_COMPLETE",
        `NODE RUN START PROCESS COMPLETED PID [${process_id}] CHILD_PID [${process.id}]`,
        { parentProcessId: process_id, childProcessId: process.id }
      );
      return [{ process_id: "", error: "unable to create process" }, ProcessStatus.ERROR];
    }
    return [{ process_id: process.id }, ProcessStatus.RUNNING];
  }
}

module.exports = {
  StartProcessSystemTaskNode,
};
