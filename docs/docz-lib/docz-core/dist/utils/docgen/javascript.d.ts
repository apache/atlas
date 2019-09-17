import { Config } from '../../config/argv';
export declare const jsParser: (files: string[], config: Config) => ({
    key: string;
    value: any;
} | null)[];
