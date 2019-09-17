import { Config as Args } from '../config/argv';
import { ServerHooks } from '../lib/Bundler';
export declare const devServerConfig: (hooks: ServerHooks, args: Args) => {
    publicPath: string;
    compress: boolean;
    logLevel: string;
    clientLogLevel: string;
    contentBase: string[];
    watchContentBase: boolean;
    hot: boolean;
    quiet: boolean;
    open: boolean;
    watchOptions: {
        ignored: any;
    };
    overlay: boolean;
    host: string;
    port: number;
    historyApiFallback: {
        disableDotRule: boolean;
    };
    disableHostCheck: boolean;
    before(app: any, server: any): void;
    after(app: any): void;
};
