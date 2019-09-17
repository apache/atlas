import Config from 'webpack-chain';
import { Config as Args } from '../config/argv';
import { BabelRC } from '../config/babel';
export declare const sourceMaps: (config: Config, args: Args) => void;
export interface AddScriptLoaderOpts {
    threadLoader?: boolean;
    rule: Config.Rule;
    babelrc: BabelRC;
    args: Args;
}
export declare const js: (config: Config, args: Args, babelrc: BabelRC) => void;
export declare const ts: (config: Config, args: Args, babelrc: BabelRC) => void;
export declare const mdx: (config: Config, args: Args, babelrc: BabelRC) => void;
export declare const images: (config: Config) => void;
export declare const svg: (config: Config) => void;
export declare const media: (config: Config) => void;
export declare const fonts: (config: Config) => void;
