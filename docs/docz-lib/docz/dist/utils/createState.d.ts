import * as React from 'react';
export interface ProviderProps<T> {
    initial?: T;
}
export declare type PrevState<T> = (prevState: T) => T;
export declare type GetFn<T> = (state: T) => React.ReactNode;
export declare type Dispatch<T> = T | PrevState<T>;
export interface State<T> {
    context: React.Context<T>;
    set: (param: Dispatch<T>) => void;
    Provider: React.ComponentType<ProviderProps<T>>;
}
export declare function create<T = any>(initial: T): State<T>;
