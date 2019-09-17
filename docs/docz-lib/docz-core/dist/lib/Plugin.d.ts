import WebpackChainConfig from 'webpack-chain';
import { Config } from '../config/argv';
import { BabelRC } from '../config/babel';
export declare type SetConfig = (config: Config) => Config | Promise<Config>;
export declare type ModifyBundlerConfig<C = any> = (config: C, dev: boolean, args: Config) => C;
export declare type ModifyBabelRC = (babelrc: BabelRC, args: Config) => BabelRC;
export declare type ModifyFiles = (files: string[], args: Config) => string[];
export declare type OnCreateWebpackChain = (config: WebpackChainConfig, dev: boolean, args: Config) => void;
export declare type onPreCreateApp = <A>(app: A) => void;
export declare type onCreateApp = <A>(app: A) => void;
export declare type OnServerListening = <S>(server: S) => void;
export declare type OnPreBuild = (args: Config) => void;
export declare type OnPostBuild = (args: Config) => void;
export declare type OnPreRender = () => void;
export declare type OnPostRender = () => void;
export interface PluginFactory {
    setConfig?: SetConfig;
    modifyBundlerConfig?: ModifyBundlerConfig;
    modifyBabelRc?: ModifyBabelRC;
    modifyFiles?: ModifyFiles;
    onCreateWebpackChain?: OnCreateWebpackChain;
    onPreCreateApp?: onPreCreateApp;
    onCreateApp?: onCreateApp;
    onServerListening?: OnServerListening;
    onPreBuild?: OnPreBuild;
    onPostBuild?: OnPostBuild;
    onPreRender?: OnPreRender;
    onPostRender?: OnPostRender;
}
export declare class Plugin<C = any> implements PluginFactory {
    static runPluginsMethod(plugins: Plugin[] | undefined): (method: keyof Plugin, ...args: any[]) => void;
    static propsOfPlugins(plugins: Plugin[]): (prop: keyof Plugin) => any[];
    static reduceFromPlugins<C>(plugins: Plugin[] | undefined): (method: keyof Plugin, initial: C, ...args: any[]) => C;
    static reduceFromPluginsAsync<C>(plugins: Plugin[] | undefined): (method: keyof Plugin, initial: C, ...args: any[]) => Promise<C>;
    readonly setConfig?: SetConfig;
    readonly modifyBundlerConfig?: ModifyBundlerConfig<C>;
    readonly modifyBabelRc?: ModifyBabelRC;
    readonly modifyFiles?: ModifyFiles;
    readonly onCreateWebpackChain?: OnCreateWebpackChain;
    readonly onPreCreateApp?: onPreCreateApp;
    readonly onCreateApp?: onCreateApp;
    readonly onServerListening?: OnServerListening;
    readonly onPreBuild?: OnPreBuild;
    readonly onPostBuild?: OnPostBuild;
    readonly onPreRender?: OnPreRender;
    readonly onPostRender?: OnPostRender;
    constructor(p: PluginFactory);
}
export declare function createPlugin<C = any>(factory: PluginFactory): Plugin<C>;
