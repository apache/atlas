import { Heading } from 'docz-utils/lib/mdast';
import { Config } from '../config/argv';
export interface EntryObj {
    id: string;
    filepath: string;
    link: string | null;
    slug: string;
    name: string;
    route: string;
    menu: string | null;
    headings: Heading[];
    [key: string]: any;
}
export declare class Entry {
    readonly [key: string]: any;
    id: string;
    filepath: string;
    link: string | null;
    slug: string;
    route: string;
    name: string;
    menu: string | null;
    headings: Heading[];
    settings: {
        [key: string]: any;
    };
    constructor(ast: any, file: string, src: string, config: Config);
    setLink(url: string): void;
    private getFilepath;
    private getName;
    private slugify;
    private getRoute;
}
