import WS from 'ws';
export declare type Send = (type: string, payload: any) => void;
export declare type On = (type: string) => Promise<any>;
export declare class Socket {
    private client?;
    constructor(server?: any, host?: string, port?: number);
    onConnection(listener: (socket: WS, emit: Send) => () => void): void;
}
