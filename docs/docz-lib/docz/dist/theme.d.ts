import { ComponentType as CT } from 'react';
import { Database, ThemeConfig, TransformFn } from './state';
export interface ThemeProps {
    db: Database;
    wrapper?: CT;
    linkComponent?: CT;
    children(WrappedComponent: CT): JSX.Element;
}
export declare type ThemeReturn = (WrappedComponent: CT) => CT<ThemeProps>;
export declare function theme(themeConfig: ThemeConfig, transform?: TransformFn): ThemeReturn;
