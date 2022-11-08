function* generateInput(v: any[]) {
    for (let i = 0; i < v.length; i++) {
        yield v[i]
    }
}

const {gen} = new class {
    count = 0;
    idx = new Map<number, number>();
    gen = (key: number, arr: any[]) => {
        let idx = this.idx.get(key)
        if (idx == undefined) {
            this.idx.set(key, arr.length - 1)
            return arr[arr.length - 1]
        } else {
            if (idx > 0) {
                idx--
                this.idx.set(key, idx)
            }
            return arr[idx]
        }
    }
}

export const combineInputs = async (vars: any[][], cb: (vars: any[]) => void) => {
    let gen = vars.map((v) => generateInput(v))
    let push = new Array<any>(vars.length);
    let closed = new Array<boolean>(vars.length);
    let left = vars.length
    let j = 0;
    while (left > 0 && j < 100) {
        for (let i = 0; i < vars.length; i++) {
            j++
            if (closed[i]) {
                continue
            }
            const v = gen[i].next()
            if (v.done) {
                left -= 1
                closed[i] = true
            } else {
                push[i] = v.value
            }
        }
        await cb(push)
    }
}