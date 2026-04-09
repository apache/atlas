import { SFC, ComponentType } from 'react';
interface Props {
    as: ComponentType<any>;
    getInitialProps?: (props: any) => any;
}
export declare const AsyncComponent: SFC<Props>;
export {};
