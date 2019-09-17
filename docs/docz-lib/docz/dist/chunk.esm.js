import { createContext, createElement, useContext, Fragment, Component, useEffect, useMemo, useRef, useState } from 'react';
import _get from 'lodash/fp/get';
import _omit from 'lodash/fp/omit';
import equal from 'fast-deep-equal';
import _merge from 'lodash/fp/merge';
import sort from 'array-sort';
import _unionBy from 'lodash/fp/unionBy';
import _flattenDepth from 'lodash/fp/flattenDepth';
import _pipe from 'lodash/fp/pipe';
import { ulid } from 'ulid';
import match from 'match-sorter';
import _throttle from 'lodash/fp/throttle';

const DefaultNotFound = () => createElement(Fragment, null, "Not found");

const DefaultLoading = () => createElement(Fragment, null, "Loading");

const DefaultPage = ({
  children
}) => createElement(Fragment, null, children);

const DefaultPlayground = ({
  component,
  code
}) => createElement(Fragment, null, component, code);

const defaultComponents = {
  loading: DefaultLoading,
  playground: DefaultPlayground,
  notFound: DefaultNotFound,
  page: DefaultPage
};
const ctx = createContext({});
const ComponentsProvider = ({
  components: themeComponents = {},
  children
}) => createElement(ctx.Provider, {
  value: Object.assign({}, defaultComponents, themeComponents)
}, children);
const useComponents = () => {
  return useContext(ctx);
};

const isFn = value => typeof value === 'function';
function flatArrFromObject(arr, prop) {
  const reducer = (arr, obj) => {
    const value = _get(prop)(obj);

    return value ? arr.concat([value]) : arr;
  };

  return Array.from(new Set(arr.reduce(reducer, [])));
}
function compare(a, b, reverse) {
  if (a < b) return reverse ? 1 : -1;
  if (a > b) return reverse ? -1 : 1;
  return 0;
}

function create(initial) {
  var _a;

  const ctx = createContext(initial);
  const listeners = new Set();

  const dispatch = fn => {
    listeners.forEach(listener => listener(fn));
  };

  return {
    context: ctx,
    set: fn => dispatch(fn),
    Provider: (_a = class Provider extends Component {
      constructor() {
        super(...arguments);
        this.state = this.props.initial || initial || {};
      }

      static getDerivedStateFromProps(props, state) {
        if (!equal(props.initial, state)) return props.initial;
        return null;
      }

      componentDidMount() {
        listeners.add(fn => this.setState(fn));
      }

      componentWillUnmount() {
        listeners.clear();
      }

      render() {
        return createElement(ctx.Provider, {
          value: this.state
        }, this.props.children);
      }

    }, _a.displayName = 'DoczStateProvider', _a)
  };
}

const doczState = create({});

const useConfig = () => {
  const state = useContext(doczState.context);
  const {
    linkComponent,
    transform,
    config,
    themeConfig = {}
  } = state;

  const newConfig = _merge(themeConfig, config ? config.themeConfig : {});

  const transformed = transform ? transform(newConfig) : newConfig;
  return Object.assign({}, config, {
    linkComponent,
    themeConfig: transformed
  });
};

const updateState = ev => {
  const {
    type,
    payload
  } = JSON.parse(ev.data);
  const prop = type.startsWith('state.') && type.split('.')[1];

  if (prop) {
    doczState.set(state => Object.assign({}, state, {
      [prop]: payload
    }));
  }
};

const useDataServer = url => {
  useEffect(() => {
    if (!url) return;
    const socket = new WebSocket(url);
    socket.onmessage = updateState;
    return () => socket.close();
  }, []);
};

const useDocs = () => {
  const {
    entries = []
  } = useContext(doczState.context);
  const arr = entries.map(({
    value
  }) => value);
  return sort(arr, (a, b) => compare(a.name, b.name));
};

const noMenu = entry => !entry.menu;

const fromMenu = menu => entry => entry.menu === menu;

const entryAsMenu = entry => ({
  name: entry.name,
  route: entry.route,
  parent: entry.parent
});

const entriesOfMenu = (menu, entries) => entries.filter(fromMenu(menu)).map(entryAsMenu);

const parseMenu = entries => name => ({
  name,
  menu: entriesOfMenu(name, entries)
});

const menusFromEntries = entries => {
  const entriesWithoutMenu = entries.filter(noMenu).map(entryAsMenu);
  const menus = flatArrFromObject(entries, 'menu').map(parseMenu(entries));
  return _unionBy('name', menus, entriesWithoutMenu);
};

const parseItemStr = item => typeof item === 'string' ? {
  name: item
} : item;

const normalize = item => {
  const selected = parseItemStr(item);
  return Object.assign({}, selected, {
    id: selected.id || ulid(),
    parent: _get('parent', selected) || _get('parent', item),
    menu: Array.isArray(selected.menu) ? selected.menu.map(normalize) : selected.menu
  });
};

const clean = item => item.href || item.route ? _omit('menu', item) : item;

const normalizeAndClean = _pipe(normalize, clean);

const mergeMenus = (entriesMenu, configMenu) => {
  const first = entriesMenu.map(normalizeAndClean);
  const second = configMenu.map(normalizeAndClean);

  const merged = _unionBy('name', first, second);

  return merged.map(item => {
    if (!item.menu) return item;
    const found = second.find(i => i.name === item.name);
    const foundMenu = found && found.menu;
    return Object.assign({}, item, {
      menu: foundMenu ? mergeMenus(item.menu, foundMenu) : item.menu || found.menu
    });
  });
};

const UNKNOWN_POS = Infinity;

const findPos = (item, orderedList = []) => {
  const name = typeof item !== 'string' ? _get('name', item) : item;
  const pos = orderedList.findIndex(item => item === name);
  return pos !== -1 ? pos : UNKNOWN_POS;
};

const compareWithMenu = (to = []) => (a, b) => {
  const list = to.map(i => i.name || i);
  return compare(findPos(a, list), findPos(b, list));
};

const sortByName = (a, b) => {
  return a.name < b.name ? -1 : a.name > b.name ? 1 : 0;
};

const sortMenus = (first, second = []) => {
  const sorted = sort(first, compareWithMenu(second), sortByName);
  return sorted.map(item => {
    if (!item.menu) return item;
    const found = second.find(menu => menu.name === item.name);
    const foundMenu = found && found.menu;
    return Object.assign({}, item, {
      menu: foundMenu ? sortMenus(item.menu, foundMenu) : sort(item.menu, sortByName)
    });
  });
};

const search = (val, menu) => {
  const items = menu.map(item => [item].concat(item.menu || []));

  const flattened = _flattenDepth(2, items);

  const flattenedDeduplicated = [...new Set(flattened)];
  return match(flattenedDeduplicated, val, {
    keys: ['name']
  });
};

const filterMenus = (items, filter) => {
  if (!filter) return items;
  return items.filter(filter).map(item => {
    if (!item.menu) return item;
    return Object.assign({}, item, {
      menu: item.menu.filter(filter)
    });
  });
};

const useMenus = opts => {
  const {
    query = ''
  } = opts || {};
  const {
    entries,
    config
  } = useContext(doczState.context);
  if (!entries) return null;
  const arr = entries.map(({
    value
  }) => value);
  const entriesMenu = menusFromEntries(arr);
  const sorted = useMemo(() => {
    const merged = mergeMenus(entriesMenu, config.menu);
    const result = sortMenus(merged, config.menu);
    return filterMenus(result, opts && opts.filter);
  }, [entries, config]);
  return query && query.length > 0 ? search(query, sorted) : sorted;
};

const usePrevious = (value, defaultValue) => {
  const ref = useRef(defaultValue);
  useEffect(() => {
    ref.current = value;
  });
  return ref.current;
};

const isClient = typeof window === 'object';

const getSize = (initialWidth, initialHeight) => ({
  innerHeight: isClient ? window.innerHeight : initialHeight,
  innerWidth: isClient ? window.innerWidth : initialWidth,
  outerHeight: isClient ? window.outerHeight : initialHeight,
  outerWidth: isClient ? window.outerWidth : initialWidth
});

const useWindowSize = (throttleMs = 300, initialWidth = Infinity, initialHeight = Infinity) => {
  const [windowSize, setWindowSize] = useState(getSize(initialHeight, initialHeight));

  const tSetWindowResize = _throttle(throttleMs, () => setWindowSize(getSize(initialHeight, initialHeight)));

  useEffect(() => {
    window.addEventListener('resize', tSetWindowResize);
    return () => void window.removeEventListener('resize', tSetWindowResize);
  }, []);
  return windowSize;
};

export { isFn as a, useComponents as b, doczState as c, useConfig as d, useDataServer as e, useDocs as f, useMenus as g, usePrevious as h, useWindowSize as i, ComponentsProvider as j };
