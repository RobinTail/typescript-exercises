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

    public async find(query: Filter<T>): Promise<T[]> {
        return readFile(this.filename, {encoding: 'utf-8'}).then((rawData) => {
            const rows = rawData.split('\n');
            let entries: T[] = [];
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
                entries = entries.filter((entry) => this.filterByMultiFilter(entry, query));
            } else if (isTextFilter(query)) {
                entries = entries.filter((entry) => this.filterByText(entry, query));
            } else {
                entries = entries.filter((entry) => this.filterByConditionOrArray(entry, query));
            }
            return entries;
        });
    }
}
