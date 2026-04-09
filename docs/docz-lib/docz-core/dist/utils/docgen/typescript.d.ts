import { Config } from '../../config/argv';
export interface TSFile {
    text?: string;
    version: number;
}
export declare const tsParser: (files: string[], config: Config, tsconfig?: string | undefined) => any;
