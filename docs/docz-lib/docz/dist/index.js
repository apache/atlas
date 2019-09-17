'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var React = require('react');
var loadable = _interopDefault(require('@loadable/component'));
var _get = _interopDefault(require('lodash/fp/get'));
var tslib_1 = require('tslib');
var __chunk_1 = require('./chunk.js');
var _omit = _interopDefault(require('lodash/fp/omit'));
var router = require('@reach/router');
var _first = _interopDefault(require('lodash/fp/first'));
var capitalize = _interopDefault(require('capitalize'));
require('fast-deep-equal');
require('lodash/fp/merge');
require('array-sort');
require('lodash/fp/unionBy');
require('lodash/fp/flattenDepth');
require('lodash/fp/pipe');
require('ulid');
require('match-sorter');
require('lodash/fp/throttle');
var react = require('@mdx-js/react');
var createHashSource = require('hash-source');
let source = createHashSource.default();;

const BasePlayground = loadable(() => Promise.resolve(require('./Playground.js')));
const Playground = props => typeof window !== 'undefined' ? React.createElement(React.Suspense, {
  fallback: null
}, React.createElement(BasePlayground, Object.assign({}, props))) : null;

const AsyncComponent = defaultProps => {
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  const [data, setData] = React.useState({});
  React.useEffect(() => {
    const {
      getInitialProps
    } = defaultProps;

    if (getInitialProps && __chunk_1.isFn(getInitialProps)) {
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
        props = tslib_1.__rest(defaultProps, ["as", "getInitialProps"]);

  return React.createElement(Comp, Object.assign({}, props, {
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

    const ExportedComponent = props => React.createElement(AsyncComponent, Object.assign({}, props, {
      as: Component || 'div',
      getInitialProps: getInitialProps
    }));

    return ExportedComponent;
  };

  return loadable(fn, {
    fallback: React.createElement(Loading, null)
  });
};
const AsyncRoute = defaultProps => {
  const {
    asyncComponent,
    path,
    entry
  } = defaultProps,
        routeProps = tslib_1.__rest(defaultProps, ["asyncComponent", "path", "entry"]);

  const components = __chunk_1.useComponents();
  const Page = components.page;
  const Component = asyncComponent;
  const props = Object.assign({}, routeProps, {
    doc: entry
  });
  return Page ? React.createElement(Page, Object.assign({}, props), React.createElement(Component, Object.assign({}, props))) : React.createElement(Component, Object.assign({}, props));
};

const Link = React.forwardRef((defaultProps, ref) => {
  const props = _omit(['activeClassName', 'partiallyActive'], defaultProps);

  const isActive = React.useCallback(({
    isCurrent
  }) => {
    return isCurrent ? {
      className: `${props.className} active`
    } : {};
  }, [props.className]);
  return React.createElement(router.Link, Object.assign({}, props, {
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
  const components = __chunk_1.useComponents();
  const {
    props: stateProps
  } = React.useContext(__chunk_1.doczState.context);
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
  return React.createElement(PropsComponent, {
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
  const components = __chunk_1.useComponents();
  const {
    entries
  } = React.useContext(__chunk_1.doczState.context);
  const NotFound = components.notFound;
  const history = React.useMemo(() => router.createHistory(source), []);
  React.useEffect(() => {
    history.listen(goToHash);
  }, []);
  return React.createElement(react.MDXProvider, {
    components: components
  }, React.createElement(router.LocationProvider, {
    history: history
  }, React.createElement(router.Router, null, React.createElement(NotFound, {
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
    return React.createElement(AsyncRoute, Object.assign({}, props, {
      entry: entry,
      key: entry.id,
      path: entry.route,
      asyncComponent: component
    }));
  }))));
};

function theme(themeConfig, transform = c => c) {
  return WrappedComponent => {
    const Theme = React.memo(props => {
      const {
        linkComponent
      } = props;
      const {
        db,
        children,
        wrapper: Wrapper = React.Fragment
      } = props;
      const initial = Object.assign({}, db, {
        themeConfig,
        transform,
        linkComponent
      });
      return React.createElement(__chunk_1.doczState.Provider, {
        initial: initial
      }, React.createElement(Wrapper, null, React.createElement(WrappedComponent, null, children)));
    });
    Theme.displayName = WrappedComponent.displayName || 'DoczTheme';
    return Theme;
  };
}

exports.ComponentsProvider = __chunk_1.ComponentsProvider;
exports.doczState = __chunk_1.doczState;
exports.useComponents = __chunk_1.useComponents;
exports.useConfig = __chunk_1.useConfig;
exports.useDataServer = __chunk_1.useDataServer;
exports.useDocs = __chunk_1.useDocs;
exports.useMenus = __chunk_1.useMenus;
exports.usePrevious = __chunk_1.usePrevious;
exports.useWindowSize = __chunk_1.useWindowSize;
exports.AsyncRoute = AsyncRoute;
exports.Link = Link;
exports.Playground = Playground;
exports.Props = Props;
exports.Routes = Routes;
exports.loadRoute = loadRoute;
exports.theme = theme;
