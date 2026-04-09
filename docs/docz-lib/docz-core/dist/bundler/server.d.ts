import webpack from 'webpack';
import { Config as Args } from '../config/argv';
import { ServerHooks as Hooks } from '../lib/Bundler';
export declare const server: (args: Args) => (config: webpack.Configuration, hooks: Hooks) => Promise<{
    start: () => Promise<any>;
}>;
