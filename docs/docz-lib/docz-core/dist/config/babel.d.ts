import { Config, Env } from '../config/argv';
export interface BabelRC {
    presets: any[];
    plugins: any[];
    cacheDirectory?: boolean;
    babelrc?: boolean;
}
export declare const getBabelConfig: (args: Config, env: Env, typescript?: boolean | undefined) => Promise<BabelRC>;
