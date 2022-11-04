function* generateInput(v: any[]) {
    for (let i = 0; i < v.length; i++) {
        yield v[i]
    }
}

export const combineInputs = (vars: any[][], cb: (vars: any[]) => void) => {
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
        cb(push)
    }
}