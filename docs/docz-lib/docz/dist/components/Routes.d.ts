import { SFC } from 'react';
import { ComponentsMap } from '../hooks/useComponents';
import { Imports } from './AsyncRoute';
export interface RoutesProps {
    components: ComponentsMap;
    imports: Imports;
}
export declare const Routes: SFC<RoutesProps>;
