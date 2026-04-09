import { SFC, ComponentType as CT } from 'react';
import { RouteComponentProps } from '@reach/router';
import { Entry } from '../state';
export declare type PageProps = RouteComponentProps<any> & {
    doc: Entry;
};
export interface PlaygroundProps {
    className?: string;
    style?: any;
    wrapper?: CT<any>;
    components: ComponentsMap;
    component: JSX.Element;
    position: number;
    code: string;
    codesandbox: string;
    scope: Record<string, any>;
}
export declare type PlaygroundComponent = CT<PlaygroundProps>;
export interface ComponentsMap {
    loading?: CT;
    page?: CT<PageProps>;
    notFound?: CT<RouteComponentProps<any>>;
    playground?: PlaygroundComponent;
    h1?: CT<any> | string;
    h2?: CT<any> | string;
    h3?: CT<any> | string;
    h4?: CT<any> | string;
    h5?: CT<any> | string;
    h6?: CT<any> | string;
    span?: CT<any> | string;
    a?: CT<any> | string;
    ul?: CT<any> | string;
    table?: CT<any> | string;
    pre?: CT<any> | string;
    code?: CT<any> | string;
    inlineCode?: CT<any> | string;
    [key: string]: any;
}
export declare type NotFoundComponent = CT<RouteComponentProps<any>>;
export interface ComponentsProviderProps {
    components: ComponentsMap;
}
export declare const ComponentsProvider: SFC<ComponentsProviderProps>;
export declare const useComponents: () => ComponentsMap;
