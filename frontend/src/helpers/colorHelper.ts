const hex2rgb = (hex: string): [number, number, number] => {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result
        ? [parseInt(result[1], 16), parseInt(result[2], 16), parseInt(result[3], 16)]
        : [255, 255, 255];
}

const componentToHex = (component: number): string => {
    const hex = component.toString(16);
    return hex.length === 1 ? '0' + hex : hex;
}

const rgb2hex = (r: number, g: number, b: number): string => {
    return '#' + componentToHex(r) + componentToHex(g) + componentToHex(b);
}

export const lighten = (hex: string, amount: number): string => {
    const rgb = hex2rgb(hex);
    const [r, g, b] = rgb;
    const [r2, g2, b2] = [r, g, b].map(c => Math.min(255, c + amount));
    return rgb2hex(r2, g2, b2);
}

export const darken = (hex: string, amount: number): string => {
    const rgb = hex2rgb(hex);
    const [r, g, b] = rgb;
    const [r2, g2, b2] = [r, g, b].map(c => Math.max(0, c - amount));
    return rgb2hex(r2, g2, b2);
}
