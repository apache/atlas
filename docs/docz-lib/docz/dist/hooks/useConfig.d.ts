/// <reference types="react" />
import { ThemeConfig, Config } from '../state';
export interface UseConfigObj extends Config {
    themeConfig: ThemeConfig;
    linkComponent?: React.ComponentType<any>;
}
export declare const useConfig: () => UseConfigObj;
