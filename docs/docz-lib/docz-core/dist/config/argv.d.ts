import { Argv as Yargs } from 'yargs';
import { Plugin } from '../lib/Plugin';
import { BabelRC } from '../config/babel';
export declare type Env = 'production' | 'development';
export declare type ThemeConfig = Record<string, any>;
export interface DocgenConfig {
    handlers?: any[];
    resolver?: (ast: any, recast: any) => any;
    propFilter?: (prop: any) => boolean;
    searchPath: string;
}
export interface Menu {
    name: string;
    route?: string;
    href?: string;
    menu?: Menu[];
}
export interface HtmlContext {
    lang: string;
    favicon?: string;
    head?: {
        meta: any[];
        links: any[];
        raw: string;
        scripts: any[];
    };
    body?: {
        raw: string;
        scripts: any[];
    };
}
export interface Argv {
    base: string;
    src: string;
    files: string | string[];
    ignore: string[];
    watchIgnore: string;
    public: string;
    dest: string;
    editBranch: string;
    config: string;
    debug: boolean;
    clearConsole: boolean;
    typescript: boolean;
    propsParser: boolean;
    host: string;
    port: number;
    websocketPort: number;
    websocketHost: string;
    native: boolean;
    codeSandbox: boolean;
    sourcemaps: boolean;
    notUseSpecifiers: boolean;
    title: string;
    description: string;
    theme: string;
    wrapper?: string;
    indexHtml?: string;
    /** slugify separator */
    separator: string;
}
export interface Config extends Argv {
    paths: Record<string, any>;
    plugins: Plugin[];
    mdPlugins: any[];
    hastPlugins: any[];
    menu: Menu[];
    htmlContext: HtmlContext;
    themeConfig: ThemeConfig;
    docgenConfig: DocgenConfig;
    filterComponents: (files: string[]) => string[];
    modifyBundlerConfig<C>(config: C, dev: boolean, args: Config): C;
    modifyBabelRc(babelrc: BabelRC, args: Config): BabelRC;
    onCreateWebpackChain<C>(c: C, dev: boolean, args: Config): void;
}
export declare const setArgs: (yargs: Yargs<{}>) => Yargs<{
    base: any;
} & {
    source: any;
} & {
    files: any;
} & {
    ignore: any;
} & {
    public: any;
} & {
    dest: any;
} & {
    editBranch: any;
} & {
    config: any;
} & {
    title: any;
} & {
    description: any;
} & {
    theme: any;
} & {
    typescript: any;
} & {
    propsParser: any;
} & {
    wrapper: any;
} & {
    indexHtml: any;
} & {
    debug: any;
} & {
    clearConsole: any;
} & {
    host: any;
} & {
    port: any;
} & {
    websocketHost: any;
} & {
    websocketPort: any;
} & {
    native: any;
} & {
    codeSandbox: any;
} & {
    sourcemaps: any;
} & {
    separator: any;
} & {
    open: boolean;
}>;
