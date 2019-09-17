import { EntryObj } from './Entry';
import { Config } from '../config/argv';
export declare const fromTemplates: (file: string) => string;
export declare type EntryMap = Record<string, EntryObj>;
export declare class Entries {
    static writeApp(config: Config, dev: boolean): Promise<void>;
    static writeImports(map: EntryMap): Promise<void>;
    all: Map<string, EntryObj>;
    get: () => Promise<EntryMap>;
    repoEditUrl: string | null;
    constructor(config: Config);
    private getMap;
}
