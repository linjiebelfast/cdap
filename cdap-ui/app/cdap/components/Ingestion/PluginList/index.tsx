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
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import PropTypes from 'prop-types';
import ThemeWrapper from 'components/ThemeWrapper';
import { MyPipelineApi } from 'api/pipeline';
import Button from '@material-ui/core/Button';
import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import { objectQuery } from 'services/helpers';
import Popover from '@material-ui/core/Popover';
import If from 'components/If';
import { getIcon } from 'components/Ingestion/helpers';
import HorizontalCarousel from 'components/HorizontalCarousel';
const styles = (theme): StyleRules => {
  return {
    // root: { display: 'flex', flexDirection: 'column', alignItems: 'center', width: 700 },
    root: { width: '100%' },
    pluginsRow: { display: 'flex', flexDirection: 'row', flexWrap: 'wrap' },
    title: { display: 'flex', alignItems: 'center' },
    pluginCard: {
      display: 'flex',
      flexDirection: 'column',
      margin: 10,
      alignItems: 'center',
      width: 250,
      height: 255,
      flexShrink: 0,
      justifyContent: 'space-around',
    },
    pluginImageContainer: {
      display: 'flex',
      alignItems: 'center',
    },
    pluginImageBackground: {
      display: 'flex',
      width: '100%',
      minHeight: 128,
      justifyContent: 'center',
      backgroundColor: theme.palette.grey[700],
    },
    pluginIcon: {
      width: 100,
      height: 'auto',
    },
    pluginFAIcon: {
      fontSize: 64,
    },
    cardTitle: {
      padding: 15,
    },
    cardButtonsContainer: {
      display: 'flex',
      width: '100%',
      justifyContent: 'center',
    },
    targetsButton: {},
  };
};
interface IPlugin {
  name: string;
  artifact: { version: string };
  widgetJson: any;
}
interface ICodeEditorProps extends WithStyles<typeof styles> {
  plugins: IPlugin[];
  title: string;
  onPluginSelect: (plugin: IPlugin) => void;
  targetList: any;
}

const PluginListView: React.FC<ICodeEditorProps> = ({
  classes,
  plugins,
  title,
  targetList,
  onPluginSelect,
}) => {
  const [anchorEl, setAnchorEl] = React.useState(null);

  const open = Boolean(anchorEl);
  const id = open ? 'simple-popover' : null;

  function handleClick(event, plugin) {
    setAnchorEl(event.currentTarget);
    onPluginSelect(plugin);
  }
  function handleClose() {
    setAnchorEl(null);
  }

  return (
    <div className={classes.root}>
      {/* <div className={classes.pluginsRow}> */}
      <HorizontalCarousel scrollAmount={650}>
        {plugins.map((plugin) => {
          const displayName = objectQuery(plugin, 'widgetJson', 'display-name') || plugin.name;
          const iconData = objectQuery(plugin, 'widgetJson', 'icon', 'arguments', 'data');
          return (
            <Card
              key={`${plugin.name} - ${plugin.artifact.version}`}
              className={classes.pluginCard}
            >
              <div className={classes.pluginImageBackground}>
                <div className={classes.pluginImageContainer}>
                  <If condition={iconData}>
                    <img className={classes.pluginIcon} src={iconData} />
                  </If>
                  <If condition={!iconData}>
                    <div
                      className={`${classes.pluginFAIcon} fa ${getIcon(plugin.name.toLowerCase())}`}
                    ></div>
                  </If>
                </div>
              </div>
              <h3 className={classes.cardTitle}>{displayName}</h3>
              <div className={classes.cardButtonsContainer}>
                <Button
                  className={classes.targetsButton}
                  color="primary"
                  onClick={(e) => handleClick(e, plugin)}
                >
                  View Targets
                </Button>
              </div>
            </Card>
          );
        })}
      </HorizontalCarousel>
      {/* </div> */}
      <Popover
        id={id}
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
      >
        <If condition={targetList}>{targetList}</If>
      </Popover>
    </div>
  );
};

const StyledPluginList = withStyles(styles)(PluginListView);

function PluginList(props) {
  return (
    <ThemeWrapper>
      <StyledPluginList {...props} />
    </ThemeWrapper>
  );
}

(PluginList as any).propTypes = {};

export default PluginList;
