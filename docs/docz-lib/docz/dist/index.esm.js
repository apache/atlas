import * as React from 'react';
import { createElement, Suspense, useState, useEffect, forwardRef, useCallback, useContext, useMemo, memo, Fragment } from 'react';
import loadable from '@loadable/component';
import _get from 'lodash/fp/get';
import { __rest } from 'tslib';
import { a as isFn, b as useComponents, c as doczState } from './chunk.esm.js';
export { j as ComponentsProvider, c as doczState, b as useComponents, d as useConfig, e as useDataServer, f as useDocs, g as useMenus, h as usePrevious, i as useWindowSize } from './chunk.esm.js';
import _omit from 'lodash/fp/omit';
import { Link as Link$1, createHistory, LocationProvider, Router } from '@reach/router';
import _first from 'lodash/fp/first';
import capitalize from 'capitalize';
import 'fast-deep-equal';
import 'lodash/fp/merge';
import 'array-sort';
import 'lodash/fp/unionBy';
import 'lodash/fp/flattenDepth';
import 'lodash/fp/pipe';
import 'ulid';
import 'match-sorter';
import 'lodash/fp/throttle';
import { MDXProvider } from '@mdx-js/react';
import createHashSource from 'hash-source';
let source = createHashSource();

const BasePlayground = loadable(() => import('./Playground.esm.js'));
const Playground = props => typeof window !== 'undefined' ? createElement(Suspense, {
  fallback: null
}, createElement(BasePlayground, Object.assign({}, props))) : null;

const AsyncComponent = defaultProps => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState({});
  useEffect(() => {
    const {
      getInitialProps
    } = defaultProps;

    if (getInitialProps && isFn(getInitialProps)) {
      setLoading(true);
      getInitialProps(defaultProps).then(data => {
        setLoading(false);
        setError(null);
        setData(data);
      }).catch(err => {
        setLoading(false);
        setError(err);
        setData({});
      });
    }
  }, []);

  const {
    as: Comp,
    getInitialProps
  } = defaultProps,
        props = __rest(defaultProps, ["as", "getInitialProps"]);

  return createElement(Comp, Object.assign({}, props, {
    data: Object.assign({}, data, {
      loading,
      error
    })
  }));
};

const loadRoute = (path, imports, components) => {
  const Loading = components.loading;

  const fn = async () => {
    const importFn = _get(path, imports);

    const {
      default: Component,
      getInitialProps
    } = await importFn();

    const ExportedComponent = props => createElement(AsyncComponent, Object.assign({}, props, {
      as: Component || 'div',
      getInitialProps: getInitialProps
    }));

    return ExportedComponent;
  };

  return loadable(fn, {
    fallback: createElement(Loading, null)
  });
};
const AsyncRoute = defaultProps => {
  const {
    asyncComponent,
    path,
    entry
  } = defaultProps,
        routeProps = __rest(defaultProps, ["asyncComponent", "path", "entry"]);

  const components = useComponents();
  const Page = components.page;
  const Component = asyncComponent;
  const props = Object.assign({}, routeProps, {
    doc: entry
  });
  return Page ? createElement(Page, Object.assign({}, props), createElement(Component, Object.assign({}, props))) : createElement(Component, Object.assign({}, props));
};

const Link = forwardRef((defaultProps, ref) => {
  const props = _omit(['activeClassName', 'partiallyActive'], defaultProps);

  const isActive = useCallback(({
    isCurrent
  }) => {
    return isCurrent ? {
      className: `${props.className} active`
    } : {};
  }, [props.className]);
  return createElement(Link$1, Object.assign({}, props, {
    getProps: isActive,
    ref: ref
  }));
});
Link.displayName = 'Link';

const RE_OBJECTOF = /(?:React\.)?(?:PropTypes\.)?objectOf\((?:React\.)?(?:PropTypes\.)?(\w+)\)/;

const getTypeStr = type => {
  switch (type.name.toLowerCase()) {
    case 'instanceof':
      return `Class(${type.value})`;

    case 'enum':
      if (type.computed) return type.value;
      return type.value ? type.value.map(v => `${v.value}`).join(' │ ') : type.raw;

    case 'union':
      return type.value ? type.value.map(t => `${getTypeStr(t)}`).join(' │ ') : type.raw;

    case 'array':
      return type.raw;

    case 'arrayof':
      return `Array<${getTypeStr(type.value)}>`;

    case 'custom':
      if (type.raw.indexOf('function') !== -1 || type.raw.indexOf('=>') !== -1) return 'Custom(Function)';else if (type.raw.toLowerCase().indexOf('objectof') !== -1) {
        const m = type.raw.match(RE_OBJECTOF);
        if (m && m[1]) return `ObjectOf(${capitalize(m[1])})`;
        return 'ObjectOf';
      }
      return 'Custom';

    case 'bool':
      return 'Boolean';

    case 'func':
      return 'Function';

    case 'shape':
      const shape = type.value;
      const rst = {};
      Object.keys(shape).forEach(key => {
        rst[key] = getTypeStr(shape[key]);
      });
      return JSON.stringify(rst, null, 2);

    default:
      return capitalize(type.name);
  }
};

const humanize = type => getTypeStr(type);

const getPropType = prop => {
  const propName = _get('name', prop.flowType || prop.type);

  if (!propName) return null;
  const isEnum = propName.startsWith('"') || propName === 'enum';
  const name = capitalize(isEnum ? 'enum' : propName);

  const value = _get('type.value', prop);

  if (!name) return null;

  if (isEnum && typeof value === 'string' || !prop.flowType && !isEnum && !value || prop.flowType && !prop.flowType.elements) {
    return name;
  }

  return prop.flowType ? humanize(prop.flowType) : humanize(prop.type);
};
const Props = ({
  title,
  isToggle,
  isRaw,
  of: component
}) => {
  const components = useComponents();
  const {
    props: stateProps
  } = useContext(doczState.context);
  const PropsComponent = components.props;

  const filename = _get('__filemeta.filename', component);

  const filemetaName = _get('__filemeta.name', component);

  const componentName = filemetaName || component.displayName || component.name;
  const found = stateProps && stateProps.length > 0 && stateProps.find(item => filename ? item.key === filename : item.key.includes(`${componentName}.`));
  const value = _get('value', found) || [];

  const firstDefinition = _first(value);

  const definition = value.find(i => i.displayName === componentName);

  const props = _get('props', definition || firstDefinition);

  if (!props) return null;
  if (!PropsComponent) return null;
  return createElement(PropsComponent, {
    title: title,
    isRaw: isRaw,
    isToggle: isToggle,
    props: props,
    getPropType: getPropType
  });
};

const goToHash = ({
  location
}) => {
  setTimeout(() => {
    if (location && location.hash) {
      const decodedHash = decodeURI(location.hash);
      const id = decodedHash.substring(1);
      const el = document.getElementById(id);
      if (el) el.scrollIntoView();
    }
  });
};

const Routes = ({
  imports
}) => {
  const components = useComponents();
  const {
    entries
  } = useContext(doczState.context);
  const NotFound = components.notFound;
  const history = useMemo(() => createHistory(source), []);
  useEffect(() => {
    history.listen(goToHash);
  }, []);
  return createElement(MDXProvider, {
    components: components
  }, createElement(LocationProvider, {
    history: history
  }, createElement(Router, null, createElement(NotFound, {
    default: true
  }), entries && entries.map(({
    key: path,
    value: entry
  }) => {
    const props = {
      path,
      entries,
      components
    };
    const component = loadRoute(path, imports, components);
    return createElement(AsyncRoute, Object.assign({}, props, {
      entry: entry,
      key: entry.id,
      path: entry.route,
      asyncComponent: component
    }));
  }))));
};

function theme(themeConfig, transform = c => c) {
  return WrappedComponent => {
    const Theme = memo(props => {
      const {
        linkComponent
      } = props;
      const {
        db,
        children,
        wrapper: Wrapper = Fragment
      } = props;
      const initial = Object.assign({}, db, {
        themeConfig,
        transform,
        linkComponent
      });
      return createElement(doczState.Provider, {
        initial: initial
      }, createElement(Wrapper, null, createElement(WrappedComponent, null, children)));
    });
    Theme.displayName = WrappedComponent.displayName || 'DoczTheme';
    return Theme;
  };
}

export { AsyncRoute, Link, Playground, Props, Routes, loadRoute, theme };
