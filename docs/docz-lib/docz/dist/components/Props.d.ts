import { SFC, ComponentType } from 'react';
export interface EnumValue {
    value: string;
    computed: boolean;
}
export interface FlowTypeElement {
    name: string;
    value: string;
}
export interface FlowTypeArgs {
    name: string;
    type: {
        name: string;
    };
}
export interface PropType {
    name: string;
    value?: any;
    raw?: any;
    computed?: boolean;
}
export interface FlowType extends PropType {
    elements: FlowTypeElement[];
    name: string;
    raw: string;
    type?: string;
    computed?: boolean;
    signature?: {
        arguments: FlowTypeArgs[];
        return: {
            name: string;
        };
    };
}
export interface Prop {
    required: boolean;
    description?: string;
    type: PropType;
    defaultValue?: {
        value: string;
        computed: boolean;
    };
    flowType?: FlowType;
}
export declare type ComponentWithDocGenInfo = ComponentType & {
    __docgenInfo: {
        description?: string;
        props?: Record<string, Prop>;
    };
};
export interface PropsProps {
    title?: Node;
    isRaw?: boolean;
    isToggle?: boolean;
    of: ComponentWithDocGenInfo;
}
export declare const getPropType: (prop: Prop) => any;
export interface PropsComponentProps {
    title?: Node;
    isRaw?: boolean;
    isToggle?: boolean;
    props: Record<string, Prop>;
    getPropType(prop: Prop): string;
}
export declare const Props: SFC<PropsProps>;
