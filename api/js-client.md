# Track

## Config

```typescript
interface Element {
    tag: string
    attrs: string[]
}

interface Autotrack {
    pageViews: boolean;
    selectors?: string[]
    elements?: Element[]
}

enum StorageMethod {
    Cookie,
    LocalStorage
}

enum LogLevel {
    Error,
    Info,
    Debug,
}

interface Group {
    type: string
    id: string
    properties: Map<PropertyName, PropertyValue>
}

interface Config {
    serverUrl: string
    autotrack?: Autotrack
    logLevel: LogLevel
    cookieExpiration: Date
    cookieSecure: boolean
    storage: StorageMethod
    attribution?: string[]
}

interface Persist {
    deviceId?: string
    group?: Group
    registeredProperties?: Map<PropertyName, PropertyValue>
    sessionId?: number
    userId?: string
    optedOut: boolean
}
type PropertyName = string
type PropertyValue = string | number | boolean | null | undefined

enum Transport {
    XHR,
    SendBeacon
}

interface TrackOptions {
    transport: Transport
}

enum PropertyOp {
    Set,
    SetOnce
}

interface Identity {
    appendToList(data: Map<PropertyName, PropertyValue>): void

    removeFromList(data: Map<PropertyName, PropertyValue>): void

    increment(data: Map<PropertyName, number>): void

    decrement(data: Map<PropertyName, number>): void

    set(data: Map<PropertyName, PropertyValue>): void

    setOnce(data: Map<PropertyName, PropertyValue>): void

    unset(properties: PropertyName[]): void
}

interface User extends Identity {
    identify(userId: string, props?: Map<PropertyName, PropertyValue>): void

    identify(props?: Map<PropertyName, PropertyValue>): void

    alias(newId: string, oldId: string): void

    optOut(): void

    optIn(): void
}

interface Group extends Identity {
    identify(group: string, groupId: string, props?: Map<PropertyName, PropertyValue>): void
}

interface OptiPrism {
    user: User
    group: Group

    configure(config: Config)

    page(props?: Map<PropertyName, PropertyValue>): void
    
    register(data: Map<PropertyName, PropertyValue>): void

    unregister(data: Map<PropertyName, PropertyValue>): void

    trackOnClick(el: HTMLElement, eventName: string, props?: Map<PropertyName, PropertyValue>, options?: TrackOptions): void

    track(eventName: string, properties?: Map<PropertyName, PropertyValue>, options?: TrackOptions): void

    reset(): void
}

```