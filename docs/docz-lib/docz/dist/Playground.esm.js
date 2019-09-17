import { createElement } from 'react';
import 'lodash/fp/get';
import { b as useComponents } from './chunk.esm.js';
import 'lodash/fp/omit';
import 'fast-deep-equal';
import 'lodash/fp/merge';
import 'array-sort';
import 'lodash/fp/unionBy';
import 'lodash/fp/flattenDepth';
import 'lodash/fp/pipe';
import 'ulid';
import 'match-sorter';
import 'lodash/fp/throttle';

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
  const components = useComponents();
  if (!components || !components.playground) return null;
  const props = {
    className,
    style,
    components
  };
  return createElement(components.playground, Object.assign({}, props, {
    component: children,
    wrapper: Wrapper,
    scope: __scope,
    position: __position,
    code: __code,
    codesandbox: __codesandbox
  }));
};

export default Playground;
