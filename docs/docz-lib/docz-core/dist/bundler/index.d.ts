import { Configuration as CFG } from 'webpack';
import { Bundler } from '../lib/Bundler';
import { Config as Args, Env } from '../config/argv';
export declare const bundler: (args: Args, env: Env) => Bundler<CFG>;
