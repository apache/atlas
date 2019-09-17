export declare const setEnv: (env: string) => void;
export interface RT {
    [key: string]: any;
}
export declare const getClientEnvironment: (publicUrl: string) => {
    raw: RT;
    stringified: {
        'process.env': {};
    };
};
