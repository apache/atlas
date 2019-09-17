import { SFC } from 'react';
import { Entry } from '../state';
import { ComponentsMap } from '../hooks/useComponents';
export declare type Imports = Record<string, () => Promise<any>>;
export declare const loadRoute: (path: string, imports: Record<string, () => Promise<any>>, components: ComponentsMap) => import("@loadable/component").LoadableComponent<any>;
interface AsyncRouteProps {
    asyncComponent: any;
    components: ComponentsMap;
    path: string;
    entry: Entry;
}
export declare const AsyncRoute: SFC<AsyncRouteProps>;
export {};
