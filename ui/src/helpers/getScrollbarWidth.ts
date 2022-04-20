export default () => {
    const outer = document.createElement('div')
    const inner = document.createElement('div')

    outer.style.visibility = 'hidden'
    outer.style.overflow = 'scroll'
    document.body.appendChild(outer)

    outer.appendChild(inner)
    const scrollbarWidth = (outer.offsetWidth - inner.offsetWidth)

    if (outer.parentNode) {
        outer.parentNode.removeChild(outer)
    }

    return scrollbarWidth
};
