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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import PluginList from 'components/Ingestion/PluginList';
import SinkList from 'components/Ingestion/SinkList';
import { MyPipelineApi } from 'api/pipeline';
import SourceSinkConfig from 'components/Ingestion/SourceSinkConfigurator';
import { Button, TextField } from '@material-ui/core';
import Helmet from 'react-helmet';
import { objectQuery } from 'services/helpers';
import AvailablePluginsStore from 'services/AvailablePluginsStore';
import { Modal, Paper } from '@material-ui/core';
import If from 'components/If';
const styles = (): StyleRules => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      padding: 15,
    },
    propsRenderBlock: {
      margin: '0 40px',
      width: '40%',
    },
    propsContainer: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'center',
    },
    jobInfo: { display: 'flex', flexDirection: 'column', alignItems: 'center', margin: 10 },
    instructionHeading: {
      marginTop: 10,
    },
    modalContent: {
      height: '90%',
      margin: 100,
    },
  };
};

const SOURCE_LIST = ['BigQueryTable', 'Spanner'];
const SINK_LIST = ['BigQueryTable', 'GCS'];
const AVAILABLE_PLUGINS = {
  batchsource: SOURCE_LIST,
  batchsink: SINK_LIST,
};
interface IIngestionProps extends WithStyles<typeof styles> {
  test: string;
  pluginsMap: any;
}

class IngestionView extends React.Component<IIngestionProps> {
  public state = {
    batchsource: [],
    batchsink: [],
    selectedSource: null,
    selectedSink: null,
    sourceBP: null,
    sinkBP: null,
    pipelineName: '',
    publishingPipeline: false,
    pipelineDescription: '',
    modalOpen: false,
  };
  public componentDidMount() {
    this.fetchPlugins('batchsource');
    this.fetchPlugins('batchsink');
    console.log('AvailablePluginsStore ', AvailablePluginsStore.getState());
  }

  public fetchPlugins = (extensionType, version = '6.2.0-SNAPSHOT', namespace = 'default') => {
    MyPipelineApi.fetchPlugins({
      namespace,
      pipelineType: 'cdap-data-pipeline',
      version,
      extensionType,
    }).subscribe((res) => {
      res
        // .filter((plugin) => AVAILABLE_PLUGINS[extensionType].includes(plugin.name))
        .forEach((plugin) => {
          const params = {
            namespace,
            artifactName: plugin.artifact.name,
            artifactVersion: plugin.artifact.version,
            scope: plugin.artifact.scope,
            keys: `widgets.${plugin.name}-${plugin.type}`,
          };
          MyPipelineApi.fetchWidgetJson(params).subscribe((widgetJ) => {
            const widgetJson = JSON.parse(widgetJ[`widgets.${plugin.name}-${plugin.type}`]);
            const pl = [...this.state[extensionType], { ...plugin, widgetJson }];
            this.setState({ [extensionType]: pl });
          });
        });
    });
  };

  public onPluginSelect = (plugin) => {
    let type: string;
    if (plugin.type === 'batchsource') {
      if ((this.state.selectedSource && this.state.selectedSource.name) === plugin.name) {
        return;
      }
      type = 'selectedSource';
    } else if (plugin.type === 'batchsink') {
      if ((this.state.selectedSink && this.state.selectedSink.name) === plugin.name) {
        return;
      }
      type = 'selectedSink';
    } else {
      return;
    }
    this.setState(
      {
        [type]: plugin,
        modalOpen: type === 'selectedSink' ? true : false,
      },
      () => {
        this.getPluginProps(plugin);
      }
    );
  };

  public getPluginProps = (plugin) => {
    const pluginParams = {
      namespace: 'default',
      parentArtifact: 'cdap-data-pipeline',
      version: '6.2.0-SNAPSHOT',
      extension: plugin.type,
      pluginName: plugin.name,
      scope: 'SYSTEM',
      artifactName: plugin.artifact.name,
      artifactScope: plugin.artifact.scope,
      limit: 1,
      order: 'DESC',
    };
    MyPipelineApi.getPluginProperties(pluginParams).subscribe((res) => {
      console.log('props', res);
      if (plugin.type === 'batchsource') {
        this.setState({ sourceBP: res[0] });
      } else if (plugin.type === 'batchsink') {
        this.setState({ sinkBP: res[0] });
      }
    });
  };

  public generatePipelineConfig = () => {
    // update connections and stages
    let stages = [];
    let connections = [];
    if (this.state.selectedSource && this.state.selectedSink) {
      const sourceName =
        this.state.selectedSource.label ||
        objectQuery(this.state.selectedSource, 'plugin', 'name') ||
        objectQuery(this.state.selectedSource, 'name');
      const sinkName =
        this.state.selectedSink.label ||
        objectQuery(this.state.selectedSink, 'plugin', 'name') ||
        objectQuery(this.state.selectedSink, 'name');
      stages = [
        {
          name: sourceName,
          plugin: {
            name: this.state.selectedSource.name,
            type: this.state.selectedSource.type,
            label: this.state.selectedSource.label,
            artifact: this.state.selectedSource.artifact,
            properties: this.state.selectedSource.properties,
          },
        },
        {
          name: sinkName,
          plugin: {
            name: this.state.selectedSink.name,
            type: this.state.selectedSink.type,
            label: this.state.selectedSink.label,
            artifact: this.state.selectedSink.artifact,
            properties: this.state.selectedSink.properties,
          },
        },
      ];
      connections = [
        {
          to: sinkName,
          from: sourceName,
        },
      ];
    }

    const configuration = {
      artifact: { name: 'cdap-data-pipeline', version: '6.2.0-SNAPSHOT', scope: 'SYSTEM' },
      description: 'Pipeline from Ingestion feature',
      name: this.state.pipelineName,
      config: {
        resources: {
          memoryMB: 2048,
          virtualCores: 1,
        },
        driverResources: {
          memoryMB: 2048,
          virtualCores: 1,
        },
        connections,
        comments: [],
        postActions: [],
        properties: {},
        processTimingEnabled: true,
        stageLoggingEnabled: true,
        stages,
        schedule: '0 * * * *',
        engine: 'spark',
        numOfRecordsPreview: 100,
        maxConcurrentRuns: 1,
      },
    };
    return configuration;
  };
  public publishPipeline = () => {
    this.setState({ publishingPipeline: true });
    console.log('publishing pipelinee');
    const configuration = this.generatePipelineConfig();
    console.log('pipeline configuration is ', JSON.stringify(configuration, null, '\t'));
    MyPipelineApi.publish(
      {
        namespace: 'default',
        appId: configuration.name,
      },
      configuration
    )
      .toPromise()
      .then((data) => {
        console.log(' Published pipeline ');
        this.setState({ publishingPipeline: false });
      })
      .catch((err) => {
        this.setState({ publishingPipeline: false });
        console.log('Error publishing pipeline', err);
      });
  };

  public onSourceChange = (newSource) => {
    this.setState({ selectedSource: newSource });
  };
  public onSinkChange = (newSource) => {
    this.setState({ selectedSink: newSource });
  };
  public closeModal = () => {
    this.setState({ modalOpen: false });
  };
  public render() {
    const { classes } = this.props;
    const targetList = (
      <SinkList title="Sinks" plugins={this.state.batchsink} onPluginSelect={this.onPluginSelect} />
    );
    return (
      <div className={classes.root}>
        <Helmet title={'Ingestion'} />
        <h5 className={classes.instructionHeading}>
          Select a source and target for the transfer.{' '}
        </h5>
        <PluginList
          title="Sources"
          plugins={this.state.batchsource}
          onPluginSelect={this.onPluginSelect}
          targetList={targetList}
        />

        <Modal open={this.state.modalOpen} onBackdropClick={this.closeModal}>
          <Paper className={classes.modalContent}>
            <Button color="primary" onClick={this.closeModal}>
              Close
            </Button>
            <div className={classes.jobInfo}>
              <TextField
                variant="outlined"
                label="Transfer Name"
                margin="dense"
                value={this.state.pipelineName}
                onChange={(event) => this.setState({ pipelineName: event.target.value })}
              />
              <TextField
                variant="outlined"
                label="Description"
                margin="dense"
                value={this.state.pipelineDescription}
                onChange={(event) => this.setState({ pipelineDescription: event.target.value })}
              />
              <Button
                disabled={this.state.publishingPipeline}
                color="primary"
                onClick={this.publishPipeline}
              >
                Deploy Transfer
              </Button>
            </div>
            <SourceSinkConfig
              sourceBP={this.state.sourceBP}
              selectedSource={this.state.selectedSource}
              sinkBP={this.state.sinkBP}
              selectedSink={this.state.selectedSink}
              onSourceChange={this.onSourceChange}
              onSinkChange={this.onSinkChange}
            />
          </Paper>
        </Modal>
      </div>
    );
  }
}

const Ingestion = withStyles(styles)(IngestionView);
export default Ingestion;
