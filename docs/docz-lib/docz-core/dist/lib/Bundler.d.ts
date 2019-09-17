/// <reference types="node" />
import * as http from 'http';
import { Config as Args, Env } from '../config/argv';
export interface ServerHooks {
    onCreateWebpackChain<C>(config: C, dev: boolean, args: Args): void;
    onPreCreateApp<A>(app: A): void;
    onCreateApp<A>(app: A): void;
    onServerListening<S>(server: S): void;
}
export interface BundlerServer {
    start(): Promise<http.Server>;
}
export declare type ConfigFn<C> = (hooks: ServerHooks) => Promise<C>;
export declare type BuildFn<C> = (config: C, dist: string, publicDir: string) => void;
export declare type ServerFn<C> = (config: C, hooks: ServerHooks) => BundlerServer | Promise<BundlerServer>;
export interface BundlerConstructor<Config> {
    args: Args;
    config: ConfigFn<Config>;
    server: ServerFn<Config>;
    build: BuildFn<Config>;
}
export interface ConfigObj {
    [key: string]: any;
}
export declare class Bundler<C = ConfigObj> {
    private readonly args;
    private config;
    private server;
    private builder;
    private hooks;
    constructor(params: BundlerConstructor<C>);
    mountConfig(env: Env): Promise<C>;
    createApp(config: C): Promise<BundlerServer>;
    build(config: C): Promise<void>;
}
