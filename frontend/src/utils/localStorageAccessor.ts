export class LocalStorageAccessor {
    private readonly propertyName: string

    constructor(propertyName: string) {
        this.propertyName = propertyName
    }

    get value(): string | null {
        return localStorage.getItem(this.propertyName)
    }

    set value(value: string | null) {
        if (value) {
            localStorage.setItem(this.propertyName, value)
        } else {
            localStorage.removeItem(this.propertyName)
        }
    }
}
