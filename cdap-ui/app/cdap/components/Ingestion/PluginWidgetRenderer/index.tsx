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
import { MyPipelineApi } from 'api/pipeline';
import ConfigurationGroup from 'components/ConfigurationGroup';
import If from 'components/If';
import { Card, TextField } from '@material-ui/core';
import { objectQuery } from 'services/helpers';
import { getIcon } from 'components/Ingestion/helpers';

const styles = (): StyleRules => {
  return {
    root: {
      height: 'auto',
      padding: 20,
    },
    labelContainer: {
      padding: 10,
      paddingLeft: 0,
      width: '100%',
    },
    heading: {
      // textAlign: 'center',
      display: 'flex',
      justifyContent: 'space-between',
    },
    pluginIconContainer: {
      display: 'flex',
      alignSelf: 'right',
    },
    pluginFAIcon: {
      fontSize: 32,
    },
    headingTitle: {
      display: 'flex',
      flexDirection: 'column',
    },
    pluginIcon: { width: 64 },
  };
};

interface IPluginWidgetRendererProps extends WithStyles<typeof styles> {
  cgProps: any;
  title: string;
  plugin: any;
}

class PluginWidgetRendererView extends React.Component<IPluginWidgetRendererProps> {
  public render() {
    const { classes, cgProps, title } = this.props;
    const pluginLabel = cgProps.widgetJson ? cgProps.widgetJson['display-name'] : '';
    const iconData = objectQuery(cgProps, 'widgetJson', 'icon', 'arguments', 'data');
    return (
      <Card className={classes.root}>
        <div className={classes.heading}>
          <div className={classes.headingTitle}>
            <h5>{title.toUpperCase()}</h5>
            <h4>{pluginLabel}</h4>
          </div>
          <div className={classes.pluginIconContainer}>
            <If condition={iconData}>
              <img className={classes.pluginIcon} src={iconData} />
            </If>
            <If condition={!iconData}>
              <div
                className={`${classes.pluginFAIcon} fa ${getIcon(
                  this.props.plugin.name.toLowerCase()
                )}`}
              ></div>
            </If>
          </div>
        </div>
        <div className={classes.labelContainer}>
          <h2>Label</h2>
          <TextField
            label="Label"
            variant="outlined"
            margin="dense"
            fullWidth={true}
            value={cgProps.label}
            onChange={(event) => cgProps.onLabelChange(event.target.value)}
          />
        </div>
        <If condition={cgProps.pluginProperties && cgProps.widgetJson}>
          <ConfigurationGroup
            pluginProperties={cgProps.pluginProperties && cgProps.pluginProperties}
            widgetJson={cgProps.widgetJson && cgProps.widgetJson}
            values={cgProps.values}
            onChange={cgProps.onChange}
            validateProperties={(cb) => {
              console.log('validating props');
              cb();
            }}
          />
        </If>
      </Card>
    );
  }
}

const PluginWidgetRenderer = withStyles(styles)(PluginWidgetRendererView);
export default PluginWidgetRenderer;
