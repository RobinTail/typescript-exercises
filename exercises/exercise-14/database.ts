import * as fs from 'fs';
import {promisify} from 'util'
const readFile = promisify(fs.readFile);

const ExistenceCharacter = 'E';

type ConditionalFilter<T> = {[K in keyof T]?: {
    $eq?: T[K];
    $gt?: T[K];
    $lt?: T[K];
    $in?: T[K][];
}};
type MultiFilter<T> = {[K in '$and' | '$or']?: ConditionalFilter<T>[]};
type TextFilter = {$text: string};
type Filter<T> = ConditionalFilter<T> | MultiFilter<T> | TextFilter;

type SortOptions<T> = {[K in keyof T]?: 1 | -1};
type ProjectionOptions<T> = {[K in keyof T]?: 1};

interface FindOptions<T> {
    sort?: SortOptions<T>;
    projection?: ProjectionOptions<T>;
}

function isTextFilter<T>(filter: Filter<T>): filter is TextFilter {
    return filter.hasOwnProperty('$text');
}

function isMultiFilter<T>(filter: Filter<T>): filter is MultiFilter<T> {
    return filter.hasOwnProperty('$and') || filter.hasOwnProperty('$or');
}

export class Database<T> {
    protected filename: string;
    protected fullTextSearchFieldNames: (keyof T)[];

    public constructor(filename: string, fullTextSearchFieldNames: (keyof T)[]) {
        this.filename = filename;
        this.fullTextSearchFieldNames = fullTextSearchFieldNames;
    }

    protected filterByConditionOrArray(entry: T, filter: ConditionalFilter<T>): boolean {
        let result = true;
        for (let field in filter) {
            if (filter.hasOwnProperty(field)) {
                const condition = filter[field]!;
                if (condition.$eq) {
                    result = result && entry[field] === condition.$eq;
                }
                if (condition.$lt) {
                    result = result && entry[field] < condition.$lt;
                }
                if (condition.$gt) {
                    result = result && entry[field] > condition.$gt;
                }
                if (condition.$in) {
                    result = result && condition.$in.includes(entry[field]);
                }
            }
        }
        return result;
    }

    protected filterByMultiFilter(entry: T, filter: MultiFilter<T>): boolean {
        if (filter.$and) {
            return filter.$and.reduce((carry: boolean, condition) =>
              carry && this.filterByConditionOrArray(entry, condition),
              true);
        } else {
            return filter.$or!.reduce((carry: boolean, condition) =>
              carry || this.filterByConditionOrArray(entry, condition),
              false);
        }
    }

    protected filterByText(entry: T, filter: TextFilter): boolean {
        let result = false;
        const words = filter.$text.split(/\s/);
        for (let word of words) {
            const regExp = new RegExp('\\b' + word + '\\b', 'i');
            for (let field of this.fullTextSearchFieldNames) {
                if (typeof entry[field] === 'string') {
                    result = result || regExp.test(entry[field] as unknown as string);
                }
            }
        }
        return result;
    }

    protected sort(a: T, b: T, sortOptions: SortOptions<T>): number {
        for (let field of Object.keys(sortOptions) as (keyof T)[]) {
            const direction: number = sortOptions[field]!;
            if (a[field] > b[field]) {
                return direction;
            } else if (a[field] < b[field]) {
                return -direction;
            }
        }
        return 0;
    }

    protected project(entry: T, projectionOptions: ProjectionOptions<T>): Partial<T> {
        let newEntry: Partial<T> = {};
        for (let field of Object.keys(projectionOptions) as (keyof T)[]) {
            newEntry = {
                ...newEntry,
                [field]: entry[field]
            };
        }
        return newEntry;
    }

    public async find(query: Filter<T>, options?: FindOptions<T>): Promise<Partial<T>[]> {
        return readFile(this.filename, {encoding: 'utf-8'}).then((rawData) => {
            const rows = rawData.split('\n');
            let entries: Partial<T>[] = [];
            for (let row of rows) {
                const isExist = row.charAt(0) === ExistenceCharacter;
                if (!isExist) {
                    continue;
                }
                entries.push(
                  JSON.parse(row.substr(1))
                );
            }
            if (isMultiFilter(query)) {
                entries = entries.filter((entry) => this.filterByMultiFilter(entry as T, query));
            } else if (isTextFilter(query)) {
                entries = entries.filter((entry) => this.filterByText(entry as T, query));
            } else {
                entries = entries.filter((entry) => this.filterByConditionOrArray(entry as T, query));
            }
            if (options) {
                if (options.sort) {
                    entries = entries.sort((a, b) =>
                      this.sort(a as T, b as T, options.sort!));
                }
                if (options.projection) {
                    entries = entries.map((entry) => this.project(entry as T, options.projection!));
                }
            }
            return entries;
        });
    }
}
