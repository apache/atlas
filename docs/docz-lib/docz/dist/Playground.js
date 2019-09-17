'use strict';

var React = require('react');
require('lodash/fp/get');
var __chunk_1 = require('./chunk.js');
require('lodash/fp/omit');
require('fast-deep-equal');
require('lodash/fp/merge');
require('array-sort');
require('lodash/fp/unionBy');
require('lodash/fp/flattenDepth');
require('lodash/fp/pipe');
require('ulid');
require('match-sorter');
require('lodash/fp/throttle');

const Playground = ({
  className,
  style,
  wrapper: Wrapper,
  children,
  __scope,
  __position,
  __code,
  __codesandbox
}) => {
  const components = __chunk_1.useComponents();
  if (!components || !components.playground) return null;
  const props = {
    className,
    style,
    components
  };
  return React.createElement(components.playground, Object.assign({}, props, {
    component: children,
    wrapper: Wrapper,
    scope: __scope,
    position: __position,
    code: __code,
    codesandbox: __codesandbox
  }));
};

exports.default = Playground;
