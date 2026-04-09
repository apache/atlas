export interface Params {
    getState: () => Record<string, any>;
    setState: (key: string, val: any) => void;
}
export interface State {
    id: string;
    start: (params: Params) => Promise<void>;
    close: () => void;
}
export interface Action {
    type: string;
    payload: any;
}
export declare type Listener = (action: Action) => void;
export declare class DataServer {
    private states;
    private state;
    private listeners;
    constructor();
    register(states: State[]): DataServer;
    start(): Promise<void>;
    close(): void;
    onStateChange(listener: Listener): () => void;
    getState(): Map<string, any>;
    private setState;
    private writeDbFile;
    private mapToObject;
}
