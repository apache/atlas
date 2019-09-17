import { State } from '../lib/DataServer';
import { Entries } from '../lib/Entries';
import { Config } from '../config/argv';
export declare const state: (entries: Entries, config: Config, dev?: boolean | undefined) => State;
