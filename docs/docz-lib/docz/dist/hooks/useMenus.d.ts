import { MenuItem } from '../state';
declare type FilterFn = (item: MenuItem) => boolean;
export interface UseMenusParams {
    query?: string;
    filter?: FilterFn;
}
export declare const useMenus: (opts?: UseMenusParams | undefined) => MenuItem[] | null;
export {};
