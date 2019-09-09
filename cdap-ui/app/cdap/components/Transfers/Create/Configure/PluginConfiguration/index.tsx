/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import * as React from 'react';
import StepButtons from 'components/Transfers/Create/StepButtons';
import { fetchPluginInfo } from 'components/Transfers/utilities';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { transfersCreateConnect } from 'components/Transfers/Create/context';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { objectQuery } from 'services/helpers';
import intersection from 'lodash/intersection';

interface IPluginConfigurationView {
  artifactName: string;
  artifactScope: string;
  pluginName: string;
  pluginType: string;
  initValues: any;
  onNext: (config) => void;
}

const PluginConfigurationView: React.SFC<IPluginConfigurationView> = ({
  onNext,
  artifactName,
  artifactScope,
  pluginName,
  pluginType,
  initValues,
}) => {
  const [loading, setLoading] = React.useState(true);
  const [pluginInfo, setPluginInfo] = React.useState({ artifact: {}, properties: {} });
  const [widgetJson, setWidgetJson] = React.useState({});
  const [values, setValues] = React.useState(initValues);
  const [required, setRequired] = React.useState([]);

  function fetchPluginInformation() {
    fetchPluginInfo(artifactName, artifactScope, pluginName, pluginType).subscribe((res) => {
      setPluginInfo(res.pluginInfo);
      setWidgetJson(res.widgetInfo);
      setLoading(false);

      const properties = objectQuery(res, 'pluginInfo', 'properties') || {};
      const requiredProperties = Object.keys(properties).filter((propertyName) => {
        return properties[propertyName].required;
      });

      setRequired(requiredProperties);
    });
  }

  function generateStageConfig() {
    const stage = {
      name: pluginName,
      plugin: {
        name: pluginName,
        label: pluginName,
        type: pluginType,
        artifact: {
          ...pluginInfo.artifact,
        },
        properties: {
          ...values,
        },
      },
    };

    return stage;
  }

  React.useEffect(() => {
    fetchPluginInformation();
  }, []);

  if (loading) {
    return <LoadingSVGCentered />;
  }

  return (
    <div>
      <ConfigurationGroup
        pluginProperties={pluginInfo.properties}
        widgetJson={widgetJson}
        values={values}
        onChange={setValues}
      />

      <StepButtons
        onNext={onNext.bind(null, generateStageConfig(), widgetJson)}
        nextDisabled={intersection(required, Object.keys(values)).length !== required.length}
      />
    </div>
  );
};

const PluginConfiguration = transfersCreateConnect(PluginConfigurationView);
export default PluginConfiguration;
