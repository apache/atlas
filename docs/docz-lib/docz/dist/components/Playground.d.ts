import { ComponentType, SFC } from 'react';
export interface PlaygroundProps {
    className?: string;
    style?: any;
    wrapper?: ComponentType<any>;
    children: any;
    __scope: Record<string, any>;
    __position: number;
    __code: string;
    __codesandbox: string;
}
declare const Playground: SFC<PlaygroundProps>;
export default Playground;
