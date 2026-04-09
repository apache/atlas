import { Config } from '../config/argv';
export declare type tagsTemplate = (type: string) => string;
export declare const htmlTemplate: (indexHtml: string | undefined) => Promise<(data: any) => string>;
interface ParseHtmlParams {
    config: Config;
    ctx: Record<string, any>;
    dev: boolean;
    template: (props: Record<string, any>) => string;
}
export declare const parseHtml: ({ config, ctx, dev, template }: ParseHtmlParams) => any;
export {};
