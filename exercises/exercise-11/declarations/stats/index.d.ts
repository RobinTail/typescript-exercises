declare module 'stats' {
    type Comparator<T> = (a: T, b: T) => number;
    type IndexFunction = <T>(input: T[], comparator: Comparator<T>) => number;
    type ElementFunction = <T>(input: T[], comparator: Comparator<T>) => T | null;

    export const getMaxIndex: IndexFunction;
    export const getMaxElement: ElementFunction;
    export const getMinIndex: IndexFunction;
    export const getMinElement: ElementFunction;
    export const getMedianIndex: IndexFunction;
    export const getMedianElement: ElementFunction;
    export const getAverageValue: <T>(input: T[], getValue: (item: T) => number) => number;
}
