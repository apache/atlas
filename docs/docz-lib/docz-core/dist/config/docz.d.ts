import { Arguments } from 'yargs';
import { BabelRC } from '../config/babel';
import { Config, Argv } from '../config/argv';
export declare const doczRcBaseConfig: {
    htmlContext: {
        lang: string;
        favicon: string;
    };
    themeConfig: {};
    docgenConfig: {};
    filterComponents: (files: string[]) => string[];
    modifyBundlerConfig: (config: any) => any;
    modifyBabelRc: (babelrc: BabelRC) => BabelRC;
    onCreateWebpackChain: () => null;
    menu: never[];
    plugins: never[];
    mdPlugins: never[];
    hastPlugins: never[];
    ignore: string[];
};
export declare const getBaseConfig: (argv: Arguments<Argv>, custom?: Partial<Config> | undefined) => Config;
export declare const parseConfig: (argv: Arguments<Argv>, custom?: Partial<Config> | undefined) => Promise<Config>;
