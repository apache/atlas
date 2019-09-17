'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var yargs = require('yargs');
var _get = _interopDefault(require('lodash/fp/get'));
var fs = require('fs-extra');
var envDotProp = require('env-dot-prop');
var humanize = _interopDefault(require('humanize-string'));
var titleize = _interopDefault(require('titleize'));
var fs$1 = require('fs');
var path = require('path');
var resolve = require('resolve');
var logger = require('signale');
var logger__default = _interopDefault(logger);
var tslib_1 = require('tslib');
var mdast = require('docz-utils/lib/mdast');
var fs$2 = require('docz-utils/lib/fs');
var glob = _interopDefault(require('fast-glob'));
var crypto = require('crypto');
var slugify = _interopDefault(require('@sindresorhus/slugify'));
var _isFunction = _interopDefault(require('lodash/fp/isFunction'));
var pReduce = _interopDefault(require('p-reduce'));
var getPkgRepo = _interopDefault(require('get-pkg-repo'));
var findup = _interopDefault(require('find-up'));
var WS = _interopDefault(require('ws'));
var _merge = _interopDefault(require('lodash/fp/merge'));
var _omit = _interopDefault(require('lodash/fp/omit'));
var loadCfg = require('load-cfg');
var detectPort = _interopDefault(require('detect-port'));
var Config = _interopDefault(require('webpack-chain'));
var frontmatter = _interopDefault(require('remark-frontmatter'));
var remarkDocz = _interopDefault(require('remark-docz'));
var rehypeDocz = _interopDefault(require('rehype-docz'));
var slug = _interopDefault(require('rehype-slug'));
var webpack = require('webpack');
var webpack__default = _interopDefault(webpack);
var webpackBarPlugin = _interopDefault(require('webpackbar'));
var htmlMinifier = require('html-minifier');
var miniHtmlWebpack = require('mini-html-webpack-plugin');
var miniHtmlWebpack__default = _interopDefault(miniHtmlWebpack);
var manifestPlugin = _interopDefault(require('webpack-manifest-plugin'));
var watchMissingNodeModules = _interopDefault(require('react-dev-utils/WatchMissingNodeModulesPlugin'));
require('react-dev-utils/ModuleNotFoundPlugin');
var webpackBundleAnalyzer = require('webpack-bundle-analyzer');
var ctags = require('common-tags');
var TerserPlugin = require('terser-webpack-plugin');
var getCacheIdentifier = _interopDefault(require('react-dev-utils/getCacheIdentifier'));
var WebpackDevServer = _interopDefault(require('webpack-dev-server'));
var WebpackDevServerUtils = require('react-dev-utils/WebpackDevServerUtils');
var express = require('express');
var errorOverlayMiddleware = _interopDefault(require('react-dev-utils/errorOverlayMiddleware'));
var evalSourceMapMiddleware = _interopDefault(require('react-dev-utils/evalSourceMapMiddleware'));
var ignoredFiles = _interopDefault(require('react-dev-utils/ignoredFiles'));
var chalk$1 = _interopDefault(require('chalk'));
var chokidar = _interopDefault(require('chokidar'));
var equal = _interopDefault(require('fast-deep-equal'));
var get = _interopDefault(require('lodash/get'));
var _propEq = _interopDefault(require('lodash/fp/propEq'));
var externalProptypesHandler = _interopDefault(require('react-docgen-external-proptypes-handler'));
var actualNameHandler = _interopDefault(require('react-docgen-actual-name-handler'));
var reactDocgen = _interopDefault(require('react-docgen'));
var _entries = _interopDefault(require('lodash/fp/entries'));
var _contains = _interopDefault(require('lodash/fp/contains'));
var _prop = _interopDefault(require('lodash/fp/prop'));
var _isEmpty = _interopDefault(require('lodash/fp/isEmpty'));
var reactDocgenTs = _interopDefault(require('react-docgen-typescript'));
var ts$1 = _interopDefault(require('typescript'));
var spawn$1 = _interopDefault(require('cross-spawn'));

const ensureSlash = (filepath, needsSlash) => {
  const hasSlash = filepath.endsWith('/');

  if (hasSlash && !needsSlash) {
    return filepath.substr(filepath, filepath.length - 1);
  } else if (!hasSlash && needsSlash) {
    return `${filepath}/`;
  } else {
    return filepath;
  }
};
const root = fs$1.realpathSync(process.cwd());
const resolveApp = to => path.resolve(root, to);
const resolveOwn = to => path.resolve(__dirname, '../', to);
const templates = path.join(resolve.sync('./'), '../templates');
const packageJson = resolveApp('package.json');
const servedPath = base => ensureSlash(base, true);
const docz = resolveApp('.docz');
const app = path.resolve(docz, 'app/');
const cache = path.resolve(docz, 'cache/');
const appPublic = path.resolve(docz, 'public/');
const appNodeModules = resolveApp('node_modules');
const appPackageJson = resolveApp('package.json');
const appYarnLock = resolveOwn('yarn.lock');
const ownNodeModules = resolveOwn('node_modules');
const getDist = dest => path.join(root, dest);
const distPublic = dest => path.join(dest, 'public/');
const importsJs = path.resolve(app, 'imports.js');
const rootJs = path.resolve(app, 'root.jsx');
const indexJs = path.resolve(app, 'index.jsx');
const indexHtml = path.resolve(app, 'index.html');
const db = path.resolve(app, 'db.json');

var paths = /*#__PURE__*/Object.freeze({
  ensureSlash: ensureSlash,
  root: root,
  resolveApp: resolveApp,
  resolveOwn: resolveOwn,
  templates: templates,
  packageJson: packageJson,
  servedPath: servedPath,
  docz: docz,
  app: app,
  cache: cache,
  appPublic: appPublic,
  appNodeModules: appNodeModules,
  appPackageJson: appPackageJson,
  appYarnLock: appYarnLock,
  ownNodeModules: ownNodeModules,
  getDist: getDist,
  distPublic: distPublic,
  importsJs: importsJs,
  rootJs: rootJs,
  indexJs: indexJs,
  indexHtml: indexHtml,
  db: db
});

const getEnv = (val, defaultValue = null) => envDotProp.get(val, defaultValue, {
  parse: true
});

const getInitialTitle = pkg => {
  const name = _get('name', pkg) || 'MyDoc';
  return titleize(humanize(name.replace(/^@.*\//, '')));
};

const getInitialDescription = pkg => _get('description', pkg) || 'My awesome app using docz';

const setArgs = yargs => {
  const pkg = fs.readJsonSync(appPackageJson, {
    throws: false
  });
  return yargs.option('base', {
    type: 'string',
    default: getEnv('docz.base', '/')
  }).option('source', {
    alias: 'src',
    type: 'string',
    default: getEnv('docz.source', './')
  }).option('files', {
    type: 'string',
    default: getEnv('docz.files', '**/*.{md,markdown,mdx}')
  }).option('ignore', {
    type: 'array',
    default: getEnv('docz.ignore', [])
  }).option('public', {
    type: 'string',
    default: getEnv('docz.public', '/public')
  }).option('dest', {
    alias: 'd',
    type: 'string',
    default: getEnv('docz.dest', '.docz/dist')
  }).option('editBranch', {
    alias: 'eb',
    type: 'string',
    default: getEnv('docz.edit.branch', 'master')
  }).option('config', {
    type: 'string',
    default: getEnv('docz.config', '')
  }).option('title', {
    type: 'string',
    default: getEnv('docz.title', getInitialTitle(pkg))
  }).option('description', {
    type: 'string',
    default: getEnv('docz.description', getInitialDescription(pkg))
  }).option('theme', {
    type: 'string',
    default: getEnv('docz.theme', 'theme')
  }).option('typescript', {
    alias: 'ts',
    type: 'boolean',
    default: getEnv('docz.typescript', false)
  }).option('propsParser', {
    type: 'boolean',
    default: getEnv('docz.props.parser', true)
  }).option('wrapper', {
    type: 'string',
    default: getEnv('docz.wrapper', null)
  }).option('indexHtml', {
    type: 'string',
    default: getEnv('docz.index.html', null)
  }).option('debug', {
    type: 'boolean',
    default: getEnv('docz.debug', false)
  }).option('clearConsole', {
    type: 'boolean',
    default: getEnv('docz.clear.console', true)
  }).option('host', {
    type: 'string',
    default: getEnv('docz.host', '127.0.0.1')
  }).option('port', {
    alias: 'p',
    type: 'number',
    default: getEnv('docz.port', 3000)
  }).option('websocketHost', {
    type: 'string',
    default: getEnv('docz.websocket.host', '127.0.0.1')
  }).option('websocketPort', {
    type: 'number',
    default: getEnv('docz.websocket.port', 60505)
  }).option('native', {
    type: 'boolean',
    default: getEnv('docz.native', false)
  }).option('codeSandbox', {
    type: 'boolean',
    default: getEnv('docz.codeSandbox', true)
  }).option('sourcemaps', {
    type: 'boolean',
    default: getEnv('docz.sourcemaps', true)
  }).option('separator', {
    type: 'string',
    default: getEnv('docz.separator', '-')
  }).option('open', {
    alias: 'o',
    describe: 'auto open browser in dev mode',
    type: 'boolean',
    default: false
  });
};

const populateNodePath = () => {
  // We support resolving modules according to `NODE_PATH`.
  // It works similar to `NODE_PATH` in Node itself:
  // https://nodejs.org/api/modules.html#modules_loading_from_the_global_folders
  // Note that unlike in Node, only *relative* paths from `NODE_PATH` are honored.
  // Otherwise, we risk importing Node.js core modules into an app instead of Webpack shims.
  // https://github.com/facebook/create-react-app/issues/1023#issuecomment-265344421
  // We also resolve them to make sure all tools using them work consistently.
  envDotProp.set('node.path', envDotProp.get('node.path', '').split(path.delimiter).filter(folder => folder && !path.isAbsolute(folder)).map(folder => path.resolve(root, folder)).join(path.delimiter));
};

const configDotEnv = () => {
  const NODE_ENV = envDotProp.get('node.env');
  const dotenv = resolveApp('.env');
  const dotenvFiles = [`${dotenv}.${NODE_ENV}.local`, `${dotenv}.${NODE_ENV}`, // Don't include `.env.local` for `test` environment
  // since normally you expect tests to produce the same
  // results for everyone
  NODE_ENV !== 'test' && `${dotenv}.local`, dotenv]; // Load environment variables from .env* files. Suppress warnings using silent
  // if this file is missing. dotenv will never modify any environment variables
  // that have already been set.  Variable expansion is supported in .env files.
  // https://github.com/motdotla/dotenv

  dotenvFiles.filter(Boolean).forEach(dotenvFile => {
    require('dotenv').config({
      path: dotenvFile
    });
  });
};

const setEnv = env => {
  envDotProp.set('babel.env', env);
  envDotProp.set('node.env', env);
  configDotEnv();
  populateNodePath();
};
const getClientEnvironment = publicUrl => {
  const APP_TEST = /^(REACT_APP_)|(ANGULAR_APP_)|(VUE_APP_)|(DOCZ_)/i;
  const raw = Object.keys(process.env).filter(key => APP_TEST.test(key)).reduce((env, key) => {
    env[key] = process.env[key];
    return env;
  }, {
    // Useful for determining whether we’re running in production mode. Most
    // importantly, it switches React into the correct mode.
    NODE_ENV: envDotProp.get('node.env') || 'development',
    // Useful for resolving the correct path to static assets in `public`. For
    // example, <img src={process.env.PUBLIC_URL + '/img/logo.png'} />. This should
    // only be used as an escape hatch. Normally you would put images into the `src`
    // and `import` them in code to get their
    PUBLIC_URL: publicUrl
  });
  const stringified = {
    'process.env': Object.keys(raw).reduce((env, key) => {
      env[key] = JSON.stringify(raw[key]);
      return env;
    }, {})
  };
  return {
    raw,
    stringified
  };
};

const createId = file => crypto.createHash('md5').update(file).digest('hex');

const mountRoute = (base, route) => {
  if (base === '/') return route;
  const baseHasSlash = base.endsWith('/');
  if (baseHasSlash) return base.substr(0, base.length - 1) + route;
  return base + route;
};

class Entry {
  constructor(ast, file, src, config) {
    const filepath = this.getFilepath(file, src);
    const parsed = mdast.getParsedData(ast);
    const name = this.getName(filepath, parsed);
    this.id = createId(file);
    this.filepath = filepath;
    this.link = '';
    this.slug = this.slugify(filepath, config.separator);
    this.route = this.getRoute(parsed, config.base);
    this.name = name;
    this.menu = parsed.menu || '';
    this.headings = mdast.headingsFromAst(ast);
    this.settings = parsed;
  }

  setLink(url) {
    if (url) {
      this.link = url.replace('{{filepath}}', this.filepath);
    }
  }

  getFilepath(file, src) {
    const srcPath = path.resolve(root, src);
    const filepath = path.relative(srcPath, file);

    if (process.platform === 'win32') {
      return filepath.split('\\').join('/');
    }

    return filepath;
  }

  getName(filepath, parsed) {
    const filename = humanize(path.parse(filepath).name);
    return parsed && parsed.name ? parsed.name : filename;
  }

  slugify(filepath, separator) {
    const ext = path.extname(filepath);
    const fileWithoutExt = filepath.replace(ext, '');
    return slugify(fileWithoutExt, {
      separator
    });
  }

  getRoute(parsed, base) {
    const parsedRoute = _get('route', parsed);
    const route = parsedRoute || `/${this.slug}`;
    return mountRoute('/', route);
  }
}

class Plugin {
  constructor(p) {
    this.setConfig = p.setConfig;
    this.modifyBundlerConfig = p.modifyBundlerConfig;
    this.modifyBabelRc = p.modifyBabelRc;
    this.modifyFiles = p.modifyFiles;
    this.onPreCreateApp = p.onPreCreateApp;
    this.onCreateWebpackChain = p.onCreateWebpackChain;
    this.onCreateApp = p.onCreateApp;
    this.onServerListening = p.onServerListening;
    this.onPreBuild = p.onPreBuild;
    this.onPostBuild = p.onPostBuild;
    this.onPreRender = p.onPreRender;
    this.onPostRender = p.onPostRender;
  }

  static runPluginsMethod(plugins) {
    return (method, ...args) => {
      if (plugins && plugins.length > 0) {
        for (const plugin of plugins) {
          const fn = _get(method, plugin);

          _isFunction(fn) && fn(...args);
        }
      }
    };
  }

  static propsOfPlugins(plugins) {
    return prop => plugins && plugins.length > 0 ? plugins.map(p => _get(prop, p)).filter(Boolean) : [];
  }

  static reduceFromPlugins(plugins) {
    return (method, initial, ...args) => {
      return [...(plugins || [])].reduce((obj, plugin) => {
        const fn = _get(method, plugin);

        return fn && _isFunction(fn) ? fn(obj, ...args) : obj;
      }, initial);
    };
  }

  static reduceFromPluginsAsync(plugins) {
    return (method, initial, ...args) => {
      return pReduce([...(plugins || [])], (obj, plugin) => {
        const fn = _get(method, plugin);

        return Promise.resolve(fn && _isFunction(fn) ? fn(obj, ...args) : obj);
      }, initial);
    };
  }

}
function createPlugin(factory) {
  return new Plugin(factory);
}

const parseRepo = () => {
  try {
    const pkg = fs.readJsonSync(appPackageJson);
    return getPkgRepo(pkg);
  } catch (err) {
    return null;
  }
};
const getRepoUrl = () => {
  const repo = parseRepo();
  return repo && (repo.browsetemplate && repo.browsetemplate.replace('{domain}', repo.domain).replace('{user}', repo.user).replace('{project}', repo.project).replace('{/tree/committish}', '') || repo.browse && repo.browse());
};

const getBitBucketPath = (branch, relative) => {
  const querystring = `?mode=edit&spa=0&at=${branch}&fileviewer=file-view-default`;
  const filepath = path.join(`/src/${branch}`, relative, `{{filepath}}`);
  return `${filepath}${querystring}`;
};

const getTree = (repo, branch, relative) => {
  const defaultPath = path.join(`/edit/${branch}`, relative, `{{filepath}}`);
  const bitBucketPath = getBitBucketPath(branch, relative);
  if (repo && repo.type === 'bitbucket') return bitBucketPath;
  return defaultPath;
};

const getRepoEditUrl = (src, branch) => {
  try {
    const repo = parseRepo();
    const project = path.parse(findup.sync('.git')).dir;
    const root$1 = path.join(root, src);
    const relative = path.relative(project, root$1);
    const tree = getTree(repo, branch, relative);
    return repo && repo.browsetemplate && repo.browsetemplate.replace('{domain}', repo.domain).replace('{user}', repo.user).replace('{project}', repo.project).replace('{/tree/committish}', tree);
  } catch (err) {
    return null;
  }
};

const fromTemplates = file => path.join(templates, file);

const mapToObj = map => Array.from(map.entries()).reduce((obj, [key, value]) => Object.assign({}, obj, {
  [`${key}`]: value
}), {});

const matchFilesWithSrc = config => files => {
  const src = path.relative(root, config.src);
  return files.map(file => file.startsWith(src) ? file : path.join(src, file));
};

const writeAppFiles = async (config, dev) => {
  const {
    plugins,
    theme
  } = config;
  const props = Plugin.propsOfPlugins(plugins);
  const onPreRenders = props('onPreRender');
  const onPostRenders = props('onPostRender');
  const isProd = !dev;
  const root = await fs$2.compiled(fromTemplates('root.tpl.js'), {
    minimize: false
  });
  const js = await fs$2.compiled(fromTemplates('index.tpl.js'), {
    minimize: false
  });
  const websocketUrl = `ws://${config.websocketHost}:${config.websocketPort}`;
  const rawRootJs = root({
    theme,
    isProd,
    wrapper: config.wrapper,
    websocketUrl
  });
  const rawIndexJs = js({
    onPreRenders,
    onPostRenders,
    isProd
  });
  await fs.remove(rootJs);
  await fs.remove(indexJs);
  await fs$2.touch(rootJs, rawRootJs);
  await fs$2.touch(indexJs, rawIndexJs);
};

class Entries {
  static async writeApp(config, dev) {
    await fs.ensureDir(app);
    await writeAppFiles(config, dev);
  }

  static async writeImports(map) {
    const imports = await fs$2.compiled(fromTemplates('imports.tpl.js'));
    const rawImportsJs = imports({
      entries: Object.values(map)
    });
    await fs$2.touch(path.join(app, 'imports.js'), rawImportsJs);
  }

  constructor(config) {
    this.repoEditUrl = getRepoEditUrl(config.src, config.editBranch);
    this.all = new Map();

    this.get = async () => this.getMap(config);
  }

  async getMap(config) {
    const {
      src,
      files: pattern,
      ignore,
      plugins,
      mdPlugins
    } = config;
    const arr = Array.isArray(pattern) ? pattern : [pattern];
    const toMatch = matchFilesWithSrc(config);
    const files = await glob(toMatch(arr), {
      ignore: ['**/node_modules/**'].concat(ignore),
      onlyFiles: true,
      unique: true,
      nocase: true,
      matchBase: true
    });

    const createEntry = async file => {
      try {
        const ast = await mdast.parseMdx(file, mdPlugins);
        const entry = new Entry(ast, file, src, config);
        if (this.repoEditUrl) entry.setLink(this.repoEditUrl);

        const {
          settings
        } = entry,
              rest = tslib_1.__rest(entry, ["settings"]);

        return Object.assign({}, settings, rest);
      } catch (err) {
        logger.error(err);
        return null;
      }
    };

    const reduce = Plugin.reduceFromPlugins(plugins);
    const modifiedFiles = reduce('modifyFiles', files);
    const map = new Map();
    const entries = await Promise.all(modifiedFiles.map(createEntry).filter(Boolean));

    for (const entry of entries) {
      if (entry) {
        map.set(entry.filepath, entry);
      }
    }

    this.all = map;
    return mapToObj(map);
  }

}

class DataServer {
  constructor() {
    this.states = new Set();
    this.state = new Map();
    this.listeners = new Set();
  }

  register(states) {
    for (const state of states) this.states.add(state);

    return this;
  }

  async start() {
    const setState = (key, val) => this.setState(key, val);

    const getState = () => this.getState();

    await Promise.all(Array.from(this.states).map(async state => {
      if (!_isFunction(state.start)) return;
      return state.start({
        setState,
        getState
      });
    }));
  }

  close() {
    for (const state of this.states) {
      _isFunction(state.close) && state.close();
    }
  }

  onStateChange(listener) {
    this.listeners.add(listener);
    return () => this.listeners.clear();
  }

  getState() {
    return this.mapToObject(this.state);
  }

  setState(key, val) {
    const prev = _get(key, this.getState());

    const next = typeof val === 'function' ? val(prev) : val;
    this.state.set(key, next);
    this.writeDbFile();
    this.listeners.forEach(listener => {
      listener({
        type: `state.${key}`,
        payload: next
      });
    });
  }

  async writeDbFile() {
    fs.outputJSONSync(db, this.mapToObject(this.state), {
      spaces: 2
    });
  }

  mapToObject(map) {
    return Array.from(map.entries()).reduce((obj, [key, val]) => Object.assign({}, obj, {
      [key]: val
    }), {});
  }

}

const onSignal = cb => {
  const signals = ['SIGINT', 'SIGTERM'];

  for (const sig of signals) {
    process.on(sig, async () => {
      await cb();
      process.exit();
    });
  }
};

const isSocketOpened = socket => socket.readyState === WS.OPEN;

const sender = socket => (type, payload) => {
  if (socket && isSocketOpened(socket)) {
    socket.send(JSON.stringify({
      type,
      payload
    }));
  }
};

class Socket {
  constructor(server, host, port) {
    if (server) {
      this.client = new WS.Server({
        server,
        host,
        port
      });
    }
  }

  onConnection(listener) {
    if (!this.client) return;
    this.client.on('connection', socket => {
      const emit = sender(socket);
      const subs = listener(socket, emit);

      const handleClose = async () => {
        subs();
        socket.terminate();
      };

      this.client.on('close', handleClose);
      onSignal(handleClose);
    });
  }

}

const toOmit = ['_', '$0', 'version', 'help'];
const htmlContext = {
  lang: 'en',
  favicon: 'https://cdn-std.dprcdn.net/files/acc_649651/LUKiMl'
};
const doczRcBaseConfig = {
  htmlContext,
  themeConfig: {},
  docgenConfig: {},
  filterComponents: files => files.filter(filepath => /\/[A-Z]\w*\.(js|jsx|ts|tsx)$/.test(filepath)),
  modifyBundlerConfig: config => config,
  modifyBabelRc: babelrc => babelrc,
  onCreateWebpackChain: () => null,
  menu: [],
  plugins: [],
  mdPlugins: [],
  hastPlugins: [],
  ignore: ['**/readme.md', '**/changelog.md', '**/code_of_conduct.md', '**/contributing.md', '**/license.md']
};
const getBaseConfig = (argv, custom) => {
  const initial = _omit(toOmit, argv);

  const base = Object.assign({}, initial, doczRcBaseConfig, {
    paths
  });
  return _merge(base, custom);
};
const parseConfig = async (argv, custom) => {
  const port = await detectPort(argv.port);
  const websocketPort = await detectPort(argv.websocketPort);
  const defaultConfig = getBaseConfig(argv, Object.assign({
    port,
    websocketPort,
    htmlContext
  }, custom));
  const config = argv.config ? loadCfg.loadFrom(path.resolve(argv.config), defaultConfig) : loadCfg.load('docz', defaultConfig);
  const reduceAsync = Plugin.reduceFromPluginsAsync(config.plugins);
  return reduceAsync('setConfig', config);
};

class Bundler {
  constructor(params) {
    const {
      args,
      config,
      server,
      build
    } = params;
    const run = Plugin.runPluginsMethod(args.plugins);
    this.args = args;
    this.config = config;
    this.server = server;
    this.builder = build;
    this.hooks = {
      onCreateWebpackChain(config, dev, args) {
        run('onCreateWebpackChain', config, dev, args);
      },

      onPreCreateApp(app) {
        run('onPreCreateApp', app);
      },

      onCreateApp(app) {
        run('onCreateApp', app);
      },

      onServerListening(server) {
        run('onServerListening', server);
      }

    };
  }

  async mountConfig(env) {
    const {
      plugins
    } = this.args;
    const isDev = env !== 'production';
    const reduce = Plugin.reduceFromPlugins(plugins);
    const userConfig = await this.config(this.hooks);
    const config = reduce('modifyBundlerConfig', userConfig, isDev, this.args);
    return this.args.modifyBundlerConfig(config, isDev, this.args);
  }

  async createApp(config) {
    return this.server(config, this.hooks);
  }

  async build(config) {
    const dist = getDist(this.args.dest);
    const publicDir = path.join(root, this.args.public);

    if (root === path.resolve(dist)) {
      logger__default.fatal(new Error('Unexpected option: "dest" cannot be set to the current working directory.'));
      process.exit(1);
    }

    await this.builder(config, dist, publicDir);
  }

}

const excludeNodeModules = filepath => /node_modules/.test(filepath) || /@babel(?:\/|\\{1,2})runtime/.test(filepath);

const sourceMaps = (config, args) => {
  const srcPath = path.resolve(root, args.src);
  config.module.rule('sourcemaps').test(/\.(js|mjs|jsx|ts|tsx|md|mdx)$/).include.add(srcPath).add(app).end().exclude.add(excludeNodeModules).end().use('sourcemaps').loader(require.resolve('source-map-loader')).end().enforce('pre');
};

const addScriptLoaders = opts => {
  const {
    rule,
    threadLoader = true,
    babelrc,
    args
  } = opts;
  return rule.when(!args.debug, rule => rule.use('cache-loader').loader(require.resolve('cache-loader')).options({
    cacheDirectory: cache
  })).when(Boolean(threadLoader), rule => rule.use('thread-loader').loader(require.resolve('thread-loader')).options({
    workers: require('os').cpus().length - 1
  })).use('babel-loader').loader(require.resolve('babel-loader')).options(babelrc).end();
};

const js = (config, args, babelrc) => {
  const srcPath = path.resolve(root, args.src);
  const rule = config.module.rule('js').test(/\.(jsx?|mjs)$/).include.add(srcPath).add(app).end().exclude.add(excludeNodeModules).end();
  addScriptLoaders({
    rule,
    babelrc,
    args
  });
};
const ts = (config, args, babelrc) => {
  const srcPath = path.resolve(root, args.src);
  const rule = config.module.rule('ts').test(/\.tsx?$/).include.add(srcPath).add(app).end().exclude.add(excludeNodeModules).end();
  addScriptLoaders({
    rule,
    babelrc,
    args
  });
};
const mdx = (config, args, babelrc) => {
  const {
    mdPlugins,
    hastPlugins
  } = args;
  const srcPath = path.resolve(root, args.src);
  const rule = config.module.rule('mdx').test(/\.(md|markdown|mdx)$/).include.add(srcPath).add(root).add(app).end().exclude.add(excludeNodeModules).end();
  addScriptLoaders({
    rule,
    babelrc,
    args,
    threadLoader: false
  }).use('mdx-loader').loader(require.resolve('@mdx-js/loader')).options({
    remarkPlugins: mdPlugins.concat([[frontmatter, {
      type: 'yaml',
      marker: '-'
    }], remarkDocz]),
    rehypePlugins: hastPlugins.concat([[rehypeDocz, {
      root: root,
      useCodeSandbox: args.codeSandbox
    }], slug])
  });
};
const INLINE_LIMIT = 10000;
const images = config => {
  config.module.rule('images').test(/\.(png|jpe?g|gif)(\?.*)?$/).use('url-loader').loader(require.resolve('url-loader')).options({
    limit: INLINE_LIMIT,
    name: `static/img/[name].[hash:8].[ext]`
  });
};
const svg = config => {
  config.module.rule('svg').test(/\.(svg)(\?.*)?$/).use('file-loader').loader(require.resolve('file-loader')).options({
    name: `static/img/[name].[hash:8].[ext]`
  });
};
const media = config => {
  config.module.rule('media').test(/\.(mp4|webm|ogg|mp3|wav|flac|aac)(\?.*)?$/).use('url-loader').loader(require.resolve('url-loader')).options({
    limit: INLINE_LIMIT,
    name: `static/media/[name].[hash:8].[ext]`
  });
};
const fonts = config => {
  config.module.rule('fonts').test(/\.(woff2?|eot|ttf|otf)(\?.*)?$/i).use('url-loader').loader(require.resolve('url-loader')).options({
    limit: INLINE_LIMIT,
    name: `static/fonts/[name].[hash:8].[ext]`
  });
};

const wrapItems = item => Object.keys(item).map(key => `${key}="${item[key]}"`).join(' ');

const generateTags = template => (items = []) => items.map(template).join('');

const generateMetaTags = generateTags(item => `<meta ${wrapItems(item)}>`);
const generateLinkTags = generateTags(item => `<link ${wrapItems(item)}>`);
const generateScriptTags = generateTags(item => `<script ${wrapItems(item)}></script>`);

const generateRawTags = (items = []) => {
  if (typeof items === 'string' || items instanceof String) return items;
  return items.map(item => item).join('');
};

const getHtmlFilepath = indexHtml => indexHtml ? path.resolve(root, indexHtml) : fromTemplates('index.tpl.html');

const getPublicUrl = (config, dev) => {
  const prefix = config.base === '/' ? '' : config.base;
  return dev ? prefix : `${prefix}/public`;
};

const emptyLineTrim = new ctags.TemplateTag(ctags.replaceResultTransformer(/^\s*[\r\n]/gm, ''), ctags.trimResultTransformer);
const htmlTemplate = async indexHtml => fs$2.compiled(getHtmlFilepath(indexHtml), {
  minimize: false,
  escape: false
});
const parseHtml = ({
  config,
  ctx,
  dev,
  template
}) => {
  const {
    title,
    description
  } = config;
  const {
    publicPath,
    css,
    js,
    lang = 'en',
    favicon,
    head = [],
    body = [],
    trimWhitespace
  } = ctx;
  const headStr = `
    ${favicon ? `<link rel="icon" type="image/x-icon" href="${favicon}">` : ''}
    ${head.meta ? generateMetaTags(head.meta) : ''}
    ${head.links ? generateLinkTags(head.links) : ''}
    ${head.raw ? generateRawTags(head.raw) : ''}
    ${head.scripts ? generateScriptTags(head.scripts) : ''}
    ${miniHtmlWebpack.generateCSSReferences(css, publicPath)}`;
  const footerStr = `
    ${body.raw ? generateRawTags(body.raw) : ''}
    ${body.scripts ? generateScriptTags(body.scripts) : ''}
    ${miniHtmlWebpack.generateJSReferences(js, publicPath)}`;
  const doc = ctags.html(template({
    title,
    description,
    lang,
    head: headStr,
    footer: footerStr,
    publicUrl: getPublicUrl(config, dev)
  }));
  return trimWhitespace ? ctags.oneLineTrim(doc) : emptyLineTrim(doc);
};

const assets = (config, args, env) => {
  const isProd = env === 'production';
  const base = servedPath(args.base);
  const publicPath = isProd ? base : '/';
  config.plugin('assets-plugin').use(manifestPlugin, [{
    publicPath,
    fileName: 'assets.json'
  }]);
};
const analyzer = config => {
  config.plugin('bundle-analyzer').use(webpackBundleAnalyzer.BundleAnalyzerPlugin, [{
    generateStatsFile: true,
    openAnalyzer: false,
    analyzerMode: 'static'
  }]);
};
const injections = (config, args, env) => {
  const {
    stringify
  } = JSON;
  const base = servedPath(args.base);

  const plugin = require('webpack/lib/DefinePlugin');

  config.plugin('injections').use(plugin, [Object.assign({}, getClientEnvironment(base).stringified, {
    NODE_ENV: stringify(env),
    DOCZ_BASE_URL: stringify(base)
  })]);
};
const ignore = config => {
  config.plugin('ignore-plugin').use(webpack.IgnorePlugin, [/(regenerate\-unicode\-properties)|(elliptic)/, /node_modules/]);
};
const hot = config => {
  config.plugin('hot-module-replacement').use(webpack.HotModuleReplacementPlugin, [{
    multiStep: true
  }]);
};
const html = async (config, args, env) => {
  const dev = env !== 'production';
  const template = await htmlTemplate(args.indexHtml);
  config.plugin('html-plugin').use(miniHtmlWebpack__default, [{
    context: Object.assign({}, args.htmlContext, {
      trimWhitespace: true
    }),
    template: ctx => {
      const doc = parseHtml({
        ctx,
        dev,
        template,
        config: args
      });
      return dev ? doc : htmlMinifier.minify(doc, {
        removeComments: true,
        collapseWhitespace: true,
        removeRedundantAttributes: true,
        useShortDoctype: true,
        removeEmptyAttributes: true,
        removeStyleLinkTypeAttributes: true,
        keepClosingSlash: true,
        minifyJS: true,
        minifyCSS: true,
        minifyURLs: true
      });
    }
  }]);
};
const webpackBar = (config, args) => {
  config.plugin('webpackbar').use(webpackBarPlugin, [{
    name: 'Docz',
    color: '#41b883'
  }]);
};
const watchNodeModulesPlugin = config => {
  config.plugin('watch-missing-node-modules').use(watchMissingNodeModules, [appNodeModules]);
};

const minifier = (config, args) => {
  config.optimization.minimizer('js').use(TerserPlugin, [{
    terserOptions: {
      parse: {
        ecma: 8
      },
      compress: {
        ecma: 5,
        warnings: false,
        comparisons: false
      },
      mangle: {
        safari10: true
      },
      output: {
        ecma: 5,
        comments: false,
        ascii_only: true
      }
    },
    parallel: true,
    cache: !args.debug,
    sourceMap: args.sourcemaps
  }]);
};

const getBabelConfig = async (args, env, typescript) => {
  const isProd = env === 'production';
  const isDev = env === 'development';
  const localBabelRc = loadCfg.load('babel', {
    presets: [],
    plugins: []
  }, false, true);
  const presets = [[require.resolve('babel-preset-react-app'), {
    typescript,
    flow: !args.typescript
  }]];
  const defaultPlugins = [[require.resolve('babel-plugin-export-metadata'), {
    notUseSpecifiers: args.notUseSpecifiers
  }], [require.resolve('babel-plugin-named-asset-import'), {
    loaderMap: {
      svg: {
        ReactComponent: '@svgr/webpack?-prettier,-svgo![path]'
      }
    }
  }]];

  const config = _merge(localBabelRc, {
    presets,
    babelrc: false,
    cacheCompression: args.debug ? false : isProd,
    cacheDirectory: !args.debug,
    cacheIdentifier: args.debug ? null : getCacheIdentifier(isProd ? 'production' : isDev && 'development', ['docz', 'docz-core']),
    compact: isProd,
    customize: require.resolve('babel-preset-react-app/webpack-overrides'),
    plugins: defaultPlugins.concat(!isProd ? [require.resolve('react-hot-loader/babel')] : [])
  });

  const reduce = Plugin.reduceFromPlugins(args.plugins);
  const newConfig = reduce('modifyBabelRc', config, args);
  return args.modifyBabelRc(newConfig, args);
};

/* eslint-disable @typescript-eslint/camelcase */
const createConfig = (args, env) => async hooks => {
  const {
    debug
  } = args;
  const config = new Config();
  const isProd = env === 'production';
  const base = servedPath(args.base);
  const dist = getDist(args.dest);
  const srcPath = path.resolve(root, args.src);
  const publicPath = isProd ? base : '/';
  /**
   * general
   */

  config.context(root);
  config.set('mode', env);
  config.when(args.sourcemaps, cfg => cfg.devtool(isProd ? 'source-map' : 'cheap-module-eval-source-map'), cfg => cfg.devtool(false));
  config.node.merge({
    child_process: 'empty',
    dgram: 'empty',
    fs: 'empty',
    net: 'empty',
    tls: 'empty'
  });
  /**
   * output
   */

  const outputProd = output => output.filename('static/js/[name].[hash].js').sourceMapFilename('static/js/[name].[hash].js.map').chunkFilename('static/js/[name].[chunkhash:8].js');

  const outputDev = output => output.filename('static/js/[name].js').sourceMapFilename('static/js/[name].js.map');

  config.output.pathinfo(true).path(path.resolve(root, dist)).publicPath(publicPath).when(isProd, outputProd, outputDev).crossOriginLoading('anonymous').devtoolModuleFilenameTemplate(info => path.resolve(info.resourcePath).replace(/\\/g, '/'));
  /**
   * entries
   */

  config.entry('app').when(!isProd, entry => entry.add(require.resolve('react-dev-utils/webpackHotDevClient'))).add(indexJs);
  /**
   * resolve
   */

  config.resolve.set('symlinks', true);
  config.resolve.extensions.add('.web.js').add('.mjs').add('.js').add('.json').add('.web.jsx').add('.jsx').add('.mdx').end();
  config.resolve.alias.set('react-native$', 'react-native-web');

  const inYarnWorkspaces = __dirname.includes('/docz/core/docz-core');

  const doczDependenciesDir = inYarnWorkspaces ? path.join(__dirname, '../../../../node_modules') : ownNodeModules;
  config.resolve.modules.add('node_modules').add(doczDependenciesDir).add(srcPath).add(root).merge(envDotProp.get('node.path').split(path.delimiter).filter(Boolean));
  config.resolveLoader.set('symlinks', true).modules // prioritize our own
  .add('node_modules').add(doczDependenciesDir).add(root);
  /**
   * loaders
   */

  const jsBabelrc = await getBabelConfig(args, env);
  const tsBabelrc = await getBabelConfig(args, env, true);
  config.when(args.sourcemaps, cfg => sourceMaps(cfg, args));
  js(config, args, jsBabelrc);
  mdx(config, args, jsBabelrc);
  images(config);
  svg(config);
  media(config);
  fonts(config);
  await html(config, args, env);
  assets(config, args, env);
  ignore(config);
  injections(config, args, env);
  isProd && hot(config);
  config.when(debug, cfg => analyzer(cfg));
  config.when(!isProd, cfg => watchNodeModulesPlugin(cfg));
  config.when(!debug && !isProd, cfg => webpackBar(cfg, args));
  /**
   * typescript setup
   */

  config.when(args.typescript, cfg => {
    cfg.resolve.extensions.prepend('.ts').prepend('.tsx').end();
    ts(cfg, args, tsBabelrc);
  });
  /**
   * optimization
   */

  config.optimization.nodeEnv(env).namedModules(true).minimize(isProd).splitChunks({
    cacheGroups: {
      vendor: {
        test: /[\\/]node_modules[\\/]/,
        name: 'vendors',
        chunks: 'all'
      }
    }
  });
  config.performance.hints(false);
  config.when(isProd, cfg => minifier(cfg, args));
  hooks.onCreateWebpackChain(config, !isProd, args);
  args.onCreateWebpackChain(config, !isProd, args);
  return config.toConfig();
};

const devServerConfig = (hooks, args) => {
  const srcPath = path.resolve(root, args.src);
  const publicDir = path.resolve(root, args.public);
  const nonExistentDir = path.resolve(__dirname, 'non-existent');
  return {
    publicPath: '/',
    compress: true,
    logLevel: args.debug ? 'debug' : 'silent',
    clientLogLevel: args.debug ? 'info' : 'none',
    contentBase: [nonExistentDir],
    watchContentBase: true,
    hot: true,
    quiet: !args.debug,
    open: true,
    watchOptions: {
      ignored: ignoredFiles(srcPath)
    },
    overlay: false,
    host: args.host,
    port: args.port,
    historyApiFallback: {
      disableDotRule: true
    },
    disableHostCheck: true,

    before(app, server) {
      app.use('/public', express.static(publicDir));
      app.use(evalSourceMapMiddleware(server));
      app.use(errorOverlayMiddleware());
      hooks.onPreCreateApp(app);
    },

    after(app) {
      hooks.onCreateApp(app);
    }

  };
};

const useYarn = fs$1.existsSync(appYarnLock);
const server = args => async (config, hooks) => ({
  start: async () => {
    const serverConfig = devServerConfig(hooks, args);
    const protocol = process.env.HTTPS === 'true' ? 'https' : 'http';

    const appName = require(packageJson).name;

    const useTypescript = args.typescript;
    const urls = WebpackDevServerUtils.prepareUrls(protocol, args.host, args.port);
    const devSocket = {
      warnings: warnings => devServer.sockWrite(devServer.sockets, 'warnings', warnings),
      errors: errors => devServer.sockWrite(devServer.sockets, 'errors', errors)
    };
    const compiler = WebpackDevServerUtils.createCompiler({
      appName,
      config,
      devSocket,
      urls,
      useYarn,
      useTypescript,
      webpack: webpack__default
    });
    const devServer = new WebpackDevServer(compiler, serverConfig);
    return devServer.listen(args.port, args.host, err => {
      if (err) return logger__default.fatal(err);
      hooks.onServerListening(devServer);
    });
  }
});

const FSR = require('react-dev-utils/FileSizeReporter');

const formatWebpackMessages = require('react-dev-utils/formatWebpackMessages');
const {
  measureFileSizesBeforeBuild,
  printFileSizesAfterBuild
} = FSR;
const WARN_AFTER_BUNDLE_GZIP_SIZE = 512 * 1024;
const WARN_AFTER_CHUNK_GZIP_SIZE = 1024 * 1024;

const hasCiEnvVar = () => envDotProp.get('ci', false, {
  parse: true
});

const copyPublicFolder = async (dest, publicDir) => {
  if (await fs.pathExists(publicDir)) {
    await fs.copy(publicDir, distPublic(dest), {
      dereference: true,
      filter: file => file !== indexHtml
    });
  }
};

const compile = config => new Promise((resolve, reject) => {
  let compiler;

  try {
    compiler = webpack__default(config);
  } catch (err) {
    onError(err);
  }

  compiler && compiler.run((err, stats) => {
    if (err) reject(err);
    resolve(stats);
  });
});

const builder = async (config, previousFileSizes) => new Promise(async (resolve, reject) => {
  try {
    const stats = await compile(config);
    const messages = formatWebpackMessages(stats.toJson({}, true));

    if (messages.errors.length) {
      return reject(new Error(messages.errors.join('\n\n')));
    }

    if (hasCiEnvVar() && messages.warnings.length) {
      logger.warn('\nTreating warnings as errors because process.env.CI = true.\n' + 'Most CI servers set it automatically.\n');
      return reject(new Error(messages.warnings.join('\n\n')));
    }

    return resolve({
      stats,
      previousFileSizes,
      warnings: messages.warnings
    });
  } catch (err) {
    reject(err);
  }
});

const onSuccess = (dist, {
  stats,
  previousFileSizes,
  warnings
}) => {
  if (warnings.length) {
    logger.log();
    logger.warn('Compiled with warnings.\n');
    logger.warn(warnings.join('\n\n'));
    logger.warn('\nSearch for the ' + chalk$1.underline(chalk$1.yellow('keywords')) + ' to learn more about each warning.');
    logger.warn('To ignore, add ' + chalk$1.cyan('// eslint-disable-next-line') + ' to the line before.\n');
  }

  logger.log();
  logger.log(`File sizes after gzip:\n`);
  printFileSizesAfterBuild(stats, previousFileSizes, dist, WARN_AFTER_BUNDLE_GZIP_SIZE, WARN_AFTER_CHUNK_GZIP_SIZE);
  logger.log();
};

const onError = err => {
  logger.log();
  logger.fatal(err);
  process.exit(1);
  logger.log();
};

const build = async (config, dist, publicDir) => {
  const interactive = new logger.Signale({
    interactive: true,
    scope: 'build'
  });

  try {
    interactive.start('Creating an optimized bundle');
    await fs.ensureDir(dist);
    const previousFileSizes = await measureFileSizesBeforeBuild(dist);
    await fs.emptyDir(dist);
    await copyPublicFolder(dist, publicDir);
    const result = await builder(config, previousFileSizes);
    interactive.success('Build successfully created');
    onSuccess(dist, result);
  } catch (err) {
    logger.fatal(chalk$1.red('Failed to compile.\n'));
    onError(err);
  }
};

const bundler = (args, env) => new Bundler({
  args,
  build,
  config: createConfig(args, env),
  server: server(args)
});

/**
 * Copyright (c) 2015-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

var chalk = require('chalk');

var execSync = require('child_process').execSync;

var spawn = require('cross-spawn');

var opn = require('opn'); // https://github.com/sindresorhus/opn#app


var OSX_CHROME = 'google chrome';
const Actions = Object.freeze({
  NONE: 0,
  BROWSER: 1,
  SCRIPT: 2
});

function getBrowserEnv() {
  // Attempt to honor this environment variable.
  // It is specific to the operating system.
  // See https://github.com/sindresorhus/opn#app for documentation.
  const value = process.env.BROWSER;
  let action;

  if (!value) {
    // Default.
    action = Actions.BROWSER;
  } else if (value.toLowerCase().endsWith('.js')) {
    action = Actions.SCRIPT;
  } else if (value.toLowerCase() === 'none') {
    action = Actions.NONE;
  } else {
    action = Actions.BROWSER;
  }

  return {
    action,
    value
  };
}

function executeNodeScript(scriptPath, url) {
  const extraArgs = process.argv.slice(2);
  const child = spawn('node', [scriptPath, ...extraArgs, url], {
    stdio: 'inherit'
  });
  child.on('close', code => {
    if (code !== 0) {
      console.log();
      console.log(chalk.red('The script specified as BROWSER environment variable failed.'));
      console.log(chalk.cyan(scriptPath) + ' exited with code ' + code + '.');
      console.log();
      return;
    }
  });
  return true;
}

function startBrowserProcess(browser, url) {
  // If we're on OS X, the user hasn't specifically
  // requested a different browser, we can try opening
  // Chrome with AppleScript. This lets us reuse an
  // existing tab when possible instead of creating a new one.
  const shouldTryOpenChromeWithAppleScript = process.platform === 'darwin' && (typeof browser !== 'string' || browser === OSX_CHROME);

  if (shouldTryOpenChromeWithAppleScript) {
    try {
      // Try our best to reuse existing tab
      // on OS X Google Chrome with AppleScript
      execSync('ps cax | grep "Google Chrome"');
      execSync('osascript openChrome.applescript "' + encodeURI(url) + '"', {
        cwd: __dirname,
        stdio: 'ignore'
      });
      return true;
    } catch (err) {// Ignore errors.
    }
  } // Another special case: on OS X, check if BROWSER has been set to "open".
  // In this case, instead of passing `open` to `opn` (which won't work),
  // just ignore it (thus ensuring the intended behavior, i.e. opening the system browser):
  // https://github.com/facebook/create-react-app/pull/1690#issuecomment-283518768


  if (process.platform === 'darwin' && browser === 'open') {
    browser = undefined;
  } // Fallback to opn
  // (It will always open new tab)


  try {
    var options = {
      app: browser,
      wait: false
    };
    opn(url, options).catch(() => {}); // Prevent `unhandledRejection` error.

    return true;
  } catch (err) {
    return false;
  }
}
/**
 * Reads the BROWSER environment variable and decides what to do with it. Returns
 * true if it opened a browser or ran a node.js script, otherwise false.
 */


function openBrowser(url) {
  const {
    action,
    value
  } = getBrowserEnv();

  switch (action) {
    case Actions.NONE:
      // Special case: BROWSER="none" will prevent opening completely.
      return false;

    case Actions.SCRIPT:
      return executeNodeScript(value, url);

    case Actions.BROWSER:
      return startBrowserProcess(value, url);

    default:
      throw new Error('Not implemented.');
  }
}

const mapToArray = (map = []) => Object.entries(map).map(entry => entry && {
  key: entry[0],
  value: entry[1]
}).filter(Boolean);

const updateEntries = entries => async p => {
  const prev = _get('entries', p.getState());

  const map = await entries.get();

  if (map && !equal(prev, map)) {
    await Entries.writeImports(map);
    p.setState('entries', mapToArray(map));
  }
};

const state = (entries, config, dev) => {
  const src = path.relative(root, config.src);
  const files = Array.isArray(config.files) ? config.files.map(filePath => path.join(src, filePath)) : path.join(src, config.files);
  const ignored = config.watchIgnore || /(((^|[\/\\])\..+)|(node_modules))/;
  const watcher = chokidar.watch(files, {
    cwd: root,
    ignored,
    persistent: true
  });
  watcher.setMaxListeners(Infinity);
  return {
    id: 'entries',
    start: async params => {
      const update = updateEntries(entries);
      await update(params);

      if (dev) {
        watcher.on('add', async () => update(params));
        watcher.on('change', async () => update(params));
        watcher.on('unlink', async () => update(params));
        watcher.on('raw', async (event, path, details) => {
          if (details.event === 'moved' && details.type === 'directory') {
            await update(params);
          }
        });
      }
    },
    close: () => {
      watcher.close();
    }
  };
};

const getInitialConfig = config => {
  const pkg = fs.readJsonSync(appPackageJson, {
    throws: false
  });
  const repoUrl = getRepoUrl();
  return {
    title: config.title,
    description: config.description,
    menu: config.menu,
    version: get(pkg, 'version'),
    repository: repoUrl,
    native: config.native,
    codeSandbox: config.codeSandbox,
    themeConfig: config.themeConfig,
    separator: config.separator
  };
};

const update = async (params, initial, {
  config
}) => {
  const next = config ? loadCfg.loadFrom(path.resolve(config), initial, true, true) : loadCfg.load('docz', initial, true, true);
  params.setState('config', next);
};

const state$1 = (config, dev) => {
  const initial = getInitialConfig(config);
  const glob = config.config || loadCfg.finds('docz');
  const ignored = config.watchIgnore || /(((^|[\/\\])\..+)|(node_modules))/;
  const watcher = chokidar.watch(glob, {
    cwd: root,
    ignored,
    persistent: true
  });
  watcher.setMaxListeners(Infinity);
  return {
    id: 'config',
    start: async params => {
      const fn = async () => update(params, initial, config);

      await update(params, initial, config);

      if (dev) {
        watcher.on('add', fn);
        watcher.on('change', fn);
        watcher.on('unlink', fn);
      }
    },
    close: () => {
      watcher.close();
    }
  };
};

const throwError = err => {
  logger__default.fatal(`Error parsing static types`);
  logger__default.error(err);
};

const jsParser = (files, config) => {
  const resolver = config.docgenConfig.resolver || reactDocgen.resolver.findAllExportedComponentDefinitions;

  const parseFilepathProps = filepath => {
    const handlers = reactDocgen.defaultHandlers.concat([externalProptypesHandler(filepath), actualNameHandler]);

    try {
      const code = fs.readFileSync(filepath, 'utf-8');
      const props = reactDocgen.parse(code, resolver, handlers);
      return {
        key: path.normalize(filepath),
        value: props
      };
    } catch (err) {
      if (config.debug) throwError(err);
      return null;
    }
  };

  return files.map(parseFilepathProps).filter(Boolean);
};

const digest = str => crypto.createHash('md5').update(str).digest('hex');

const cacheFilepath = path.join(cache, 'propsParser.json');

const readCacheFile = () => fs.readJSONSync(cacheFilepath, {
  throws: false
});

function checkFilesOnCache(files) {
  const cache = readCacheFile();
  if (_isEmpty(cache)) return files;
  return files.filter(filepath => {
    const normalized = path.normalize(filepath);
    const hash = digest(fs.readFileSync(normalized, 'utf-8'));

    const found = _get(normalized, cache);

    return found && hash !== found.hash;
  });
}

function writePropsOnCache(items) {
  const cache = readCacheFile();
  const newCache = items.reduce((obj, {
    key: filepath,
    value
  }) => {
    const normalized = path.normalize(filepath);
    const hash = digest(fs.readFileSync(normalized, 'utf-8'));
    return Object.assign({}, obj, {
      [normalized]: {
        hash,
        props: value
      }
    });
  }, {});
  fs.outputJSONSync(cacheFilepath, Object.assign({}, cache, newCache));
}

function getPropsOnCache() {
  const cache = readCacheFile();

  if (_isEmpty(cache)) {
    logger.warn('Any cache was found with your props definitions');
    logger.warn("We'll parse your components to get props from them");
    logger.warn('Depending on your components, this could take while...');
    return [];
  }

  return Object.entries(cache).map(([key, value]) => ({
    key,
    value: _get('props', value)
  }));
}

const mergeWithCache = (cache, props) => {
  const keys = props.map(_prop('key'));
  return cache.filter(item => !_contains(item.key, keys)).concat(props);
};

const removeFromCache = filepath => {
  const cache = readCacheFile();
  fs.outputJSONSync(cacheFilepath, _omit(filepath, cache));
};

const getInitialFilesMap = () => {
  const cache = readCacheFile();
  if (_isEmpty(cache)) return new Map();
  const map = new Map();

  _entries(cache).forEach(([filepath]) => {
    const exist = fs.pathExistsSync(filepath);

    if (!exist) {
      removeFromCache(filepath);
    } else {
      map.set(filepath, {
        text: fs.readFileSync(filepath, 'utf-8'),
        version: 0
      });
    }
  });

  return map;
};

let languageService = null;
const filesMap = getInitialFilesMap();

function getTSConfigFile(tsconfigPath) {
  const basePath = path.dirname(tsconfigPath);
  const configFile = ts$1.readConfigFile(tsconfigPath, ts$1.sys.readFile);
  return ts$1.parseJsonConfigFileContent(configFile.config, ts$1.sys, basePath, {}, tsconfigPath);
}

function loadFiles(filesToLoad) {
  filesToLoad.forEach(filepath => {
    const normalized = path.normalize(filepath);
    const found = filesMap.get(normalized);
    filesMap.set(normalized, {
      text: fs.readFileSync(normalized, 'utf-8'),
      version: found ? found.version + 1 : 0
    });
  });
}

function createServiceHost(compilerOptions, files) {
  return {
    getScriptFileNames: () => {
      return [...files.keys()];
    },
    getScriptVersion: fileName => {
      const file = files.get(fileName);
      return file && file.version.toString() || '';
    },
    getScriptSnapshot: fileName => {
      if (!fs.existsSync(fileName)) {
        return undefined;
      }

      let file = files.get(fileName);

      if (file === undefined) {
        const text = fs.readFileSync(fileName).toString();
        file = {
          version: 0,
          text
        };
        files.set(fileName, file);
      }

      return ts$1.ScriptSnapshot.fromString(file.text);
    },
    getCurrentDirectory: () => process.cwd(),
    getCompilationSettings: () => compilerOptions,
    getDefaultLibFileName: options => ts$1.getDefaultLibFilePath(options),
    fileExists: ts$1.sys.fileExists,
    readFile: ts$1.sys.readFile,
    readDirectory: ts$1.sys.readDirectory
  };
}

const parseFiles = (files, config, tsconfig) => {
  const opts = {
    propFilter(prop) {
      if (prop.parent == null) return true;
      const propFilter = config.docgenConfig.propFilter;
      const val = propFilter && _isFunction(propFilter) && propFilter(prop);
      return !prop.parent.fileName.includes('node_modules') || Boolean(val);
    },

    componentNameResolver(exp, source) {
      const componentNameResolver = config.docgenConfig.resolver;
      const val = componentNameResolver && _isFunction(componentNameResolver) && componentNameResolver(exp, source);
      return val;
    }

  };
  loadFiles(files);
  const parser = reactDocgenTs.withCustomConfig(tsconfig, opts);

  const compilerOptions = _get('options', getTSConfigFile(tsconfig));

  const programProvider = () => {
    if (languageService) return languageService.getProgram();
    const servicesHost = createServiceHost(compilerOptions, filesMap);
    const documentRegistry = ts$1.createDocumentRegistry();
    languageService = ts$1.createLanguageService(servicesHost, documentRegistry);
    return languageService.getProgram();
  };

  return files.map(filepath => ({
    key: path.normalize(filepath),
    value: parser.parseWithProgramProvider(filepath, programProvider)
  }));
};

const tsParser = (files, config, tsconfig) => {
  if (!tsconfig) return null;
  const filesToLoad = checkFilesOnCache(files);
  const propsOnCache = getPropsOnCache();
  if (!filesToLoad.length) return propsOnCache;
  const next = parseFiles(filesToLoad, config, tsconfig);
  writePropsOnCache(next);
  return mergeWithCache(propsOnCache, next);
};

const docgen = async (files, config) => {
  const tsconfig = await findup('tsconfig.json', {
    cwd: root
  });
  return config.typescript ? tsParser(files, config, tsconfig) : jsParser(files, config);
};

const getPattern = config => {
  const {
    ignore,
    src: source,
    typescript: ts,
    docgenConfig: docgenConfig
  } = config;
  const src = path.relative(root, docgenConfig.searchPath ? docgenConfig.searchPath : source);
  return ignore.map(entry => `!**/${entry}`).concat([path.join(src, ts ? '**/*.{ts,tsx}' : '**/*.{js,jsx,mjs}'), '!**/node_modules', '!**/doczrc.js']);
};

const removeFilepath = (items, filepath) => items.filter(item => item.key !== filepath);

const initial = (config, pattern) => async p => {
  const {
    filterComponents
  } = config;
  const files = await glob(pattern, {
    cwd: root
  });
  const filtered = filterComponents ? filterComponents(files) : files;
  const metadata = await docgen(filtered, config);
  p.setState('props', metadata);
};

const change = (p, config) => async filepath => {
  const prev = _get('props', p.getState());

  const metadata = await docgen([filepath], config);
  const filtered = metadata.filter(_propEq('key', filepath));
  const next = removeFilepath(prev, filepath).concat(filtered);
  p.setState('props', next);
};

const remove = p => async filepath => {
  const prev = _get('props', p.getState());

  const next = removeFilepath(prev, filepath);
  p.setState('props', next);
};

const state$2 = (config, dev) => {
  const pattern = getPattern(config);
  const ignored = config.watchIgnore || /(((^|[\/\\])\..+)|(node_modules))/;
  const watcher = chokidar.watch(pattern, {
    cwd: root,
    ignored,
    persistent: true
  });
  watcher.setMaxListeners(Infinity);
  return {
    id: 'props',
    start: async params => {
      const addInitial = initial(config, pattern);
      await addInitial(params);

      if (dev) {
        watcher.on('change', change(params, config));
        watcher.on('unlink', remove(params));
      }
    },
    close: () => {
      watcher.close();
    }
  };
};



var index = /*#__PURE__*/Object.freeze({
  entries: state,
  config: state$1,
  props: state$2
});

process.setMaxListeners(Infinity);
const dev = async args => {
  const env = envDotProp.get('node.env');
  const config = await parseConfig(args);
  const bundler$1 = bundler(config, env);
  const entries = new Entries(config);
  const {
    websocketHost,
    websocketPort
  } = config;
  const bundlerConfig = await bundler$1.mountConfig(env);
  const app = await bundler$1.createApp(bundlerConfig);

  try {
    await Entries.writeApp(config, true);
    await Entries.writeImports((await entries.get()));
  } catch (err) {
    logger.fatal('Failed to build your files');
    logger.error(err);
    process.exit(1);
  }

  const server = await app.start();
  const dataServer = new DataServer();
  const socket = new Socket(server, websocketHost, websocketPort);
  if (config.propsParser) dataServer.register([state$2(config, true)]);
  dataServer.register([state$1(config, true), state(entries, config, true)]);

  try {
    await dataServer.start();
    if (args.open || args.o) openBrowser(`http://${config.host}:${config.port}`);
  } catch (err) {
    logger.fatal('Failed to process data server');
    logger.error(err);
    dataServer.close();
    process.exit(1);
  }

  socket.onConnection((_, emit) => {
    const subscribe = dataServer.onStateChange(action => {
      emit(action.type, action.payload);
    });
    return () => subscribe();
  });
  onSignal(async () => {
    dataServer.close();
    server.close();
  });
  server.on('close', async () => {
    dataServer.close();
  });
};

const build$1 = async args => {
  const env = envDotProp.get('node.env');
  const config = await parseConfig(args);
  const entries = new Entries(config);
  const bundler$1 = bundler(config, env);
  const bundlerConfig = await bundler$1.mountConfig(env);
  const run = Plugin.runPluginsMethod(config.plugins);
  const dataServer = new DataServer();
  if (config.propsParser) dataServer.register([state$2(config)]);
  dataServer.register([state$1(config), state(entries, config)]);

  try {
    await Entries.writeApp(config, false);
    await Entries.writeImports((await entries.get()));
    await dataServer.start();
    await run('onPreBuild', config);
    await bundler$1.build(bundlerConfig);
    await run('onPostBuild', config);
    dataServer.close();
  } catch (err) {
    logger.error(err);
    process.exit(1);
    dataServer.close();
  }
};

const serve = async args => {
  const config = await parseConfig(args);
  const dist = getDist(config.dest);
  spawn$1.sync('serve', ['-s', dist], {
    stdio: 'inherit'
  });
};

const cli = () => {
  return yargs.command('dev', 'initialize docz dev server', setArgs, async args => {
    setEnv('development');
    await dev(args);
  }).command('build', 'build dir as static site', setArgs, async args => {
    setEnv('production');
    await build$1(args);
    process.exit();
  }).command('serve', 'serve dir as static site', setArgs, async args => {
    setEnv('production');
    await build$1(args);
    await serve(args);
    process.exit();
  }).demandCommand().help().wrap(72).epilog('for more information visit https://github.com/pedronauck/docz').showHelpOnFail(false, 'whoops, something went wrong! run with --help').argv;
};

/** cli exports */

exports.DataServer = DataServer;
exports.Entries = Entries;
exports.Entry = Entry;
exports.Plugin = Plugin;
exports.cli = cli;
exports.createPlugin = createPlugin;
exports.getBaseConfig = getBaseConfig;
exports.parseConfig = parseConfig;
exports.setArgs = setArgs;
exports.states = index;
