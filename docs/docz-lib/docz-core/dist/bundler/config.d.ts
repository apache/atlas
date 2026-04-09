import { Configuration } from 'webpack';
import { ServerHooks as Hooks } from '../lib/Bundler';
import { Config as Args, Env } from '../config/argv';
export declare const createConfig: (args: Args, env: Env) => (hooks: Hooks) => Promise<Configuration>;
