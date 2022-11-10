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

interface Track {

}

enum StorageMethod {
    Cookie,
    LocalStorage
}

interface Attribution {
}

enum LogLevel {
    Error,
    Info,
    Debug,
}
interface Config {
    serverUrl: string
    autotrack?: Autotrack
    track?: Track
    logLevel:LogLevel
    deviceId?: string
    sessionId?: string
    userId?: string
    optedOut: boolean
    cookieExpiration: Date
    cookieSecure: boolean
    storage: StorageMethod
    attribution?: Attribution
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

    pageView(props?: Map<PropertyName, PropertyValue>): void

    pageView(pageName: string, props?: Map<PropertyName, PropertyValue>): void

    register(data: Map<PropertyName, PropertyValue>): void

    unregister(data: Map<PropertyName, PropertyValue>): void

    trackOnClick(el: HTMLElement, eventName: string, props?: Map<PropertyName, PropertyValue>, options?: TrackOptions): void

    track(eventName: string, properties?: Map<PropertyName, PropertyValue>, options?: TrackOptions): void

    reset(): void
}

```