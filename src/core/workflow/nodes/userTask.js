const _ = require("lodash");
const obju = require("../../utils/object");
const { ProcessStatus } = require("../process_state");
const { Validator } = require("../../validators");
const { getActivityManager } = require("../../utils/activity_manager_factory");
const crypto_manager = require("../../crypto_manager");
const { timeoutParse } = require("../../utils/node");
const { ParameterizedNode } = require("./parameterized");

class UserTaskNode extends ParameterizedNode {
  static get rules() {
    const parameters_rules = {
      parameters_has_action: [obju.hasField, "action"],
      timeout_has_valid_type: [obju.isFieldTypeIn, "timeout", ["undefined", "number", "object"]],
      channels_has_valid_type: [(obj, field) => obj[field] === undefined || obj[field] instanceof Array, "channels"],
      encrypted_data_has_valid_type: [
        (obj, field) => obj[field] === undefined || obj[field] instanceof Array,
        "encrypted_data",
      ],
    };
    return {
      ...super.rules,
      next_has_valid_type: [obju.isFieldTypeIn, "next", ["string", "number"]],
      parameters_nested_validations: [new Validator(parameters_rules), "parameters"],
    };
  }

  validate() {
    return UserTaskNode.validate(this._spec);
  }

  async run({ bag, input, external_input = null, actor_data, environment = {}, parameters = {} }, lisp) {
    try {
      if (!external_input) {
        const execution_data = this._preProcessing({ bag, input, actor_data, environment, parameters });

        const activity_manager = getActivityManager(this._spec.parameters.activity_manager);
        activity_manager.props = {
          result: execution_data,
          action: this._spec.parameters.action,
        };
        activity_manager.parameters = {};

        activity_manager.parameters.timeout = timeoutParse(this._spec.parameters, execution_data);

        if (this._spec.parameters.channels) {
          activity_manager.parameters.channels = this._spec.parameters.channels;
        }
        if (this._spec.parameters.encrypted_data) {
          activity_manager.parameters.encrypted_data = this._spec.parameters.encrypted_data;
        }
        if (this._spec.parameters.activity_schema) {
          activity_manager.parameters.activity_schema = this._spec.parameters.activity_schema;
        }
        let next_node_id = this.id;
        let status = ProcessStatus.WAITING;
        if (activity_manager.type === "notify") {
          next_node_id = this.next();
          status = ProcessStatus.RUNNING;
        }

        return {
          node_id: this.id,
          bag: bag,
          external_input: external_input,
          result: execution_data,
          error: null,
          status: status,
          next_node_id: next_node_id,
          activity_manager: activity_manager,
          action: this._spec.parameters.action,
          activity_schema: this._spec.parameters.activity_schema,
        };
      }
    } catch (err) {
      return this._processError(err, { bag, external_input });
    }

    if (this._spec.parameters.encrypted_data) {
      const crypto = crypto_manager.getCrypto();

      for (const field_path of this._spec.parameters.encrypted_data) {
        const data = _.get(external_input, field_path);
        if (data) {
          const encrypted_data = crypto.encrypt(data);
          _.set(external_input, field_path, encrypted_data);
        }
      }
    }
    return await this._postRun(bag, input, external_input, lisp);
  }

  async _preRun(execution_data) {
    return [execution_data, ProcessStatus.WAITING];
  }

  async _postRun(bag, input, external_input) {
    return {
      node_id: this.id,
      bag: bag,
      external_input: external_input,
      result: external_input,
      error: null,
      status: ProcessStatus.RUNNING,
      next_node_id: this.next(external_input),
    };
  }
}

module.exports = {
  UserTaskNode,
};
