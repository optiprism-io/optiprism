// export function getMultipleString(dates, names) {
//     const stringDates = dates.map((item) => {
//         return getStringDate(item, names);
//     });
//
//     return stringDates.join('; ');
// }

// export function getRangeString(from, to, names) {
//     return `${getStringDate(from, names)} - ${getStringDate(to, names)}`;
// }

// export function getMonthRangeString(from, to, names) {
//     return `${getStringDate(from, names, false)} - ${getStringDate(to, names, false)}`;
// }

export function getStringDate(item: string | number, names: string[], includeDate = true) {
    if (isNaN(new Date(item).getTime())) {
        return item;
    }
    return `${includeDate ? (new Date(item)).getUTCDate() : ''} ${names[(new Date(item)).getUTCMonth()]} ${(new Date(item)).getUTCFullYear()}`;
}

export function getShortStringDate(date: Date) {
    if (date instanceof Date) {
        return `${date.getFullYear()}-${date.getMonth() < 9 ? `0${date.getMonth() + 1}` : date.getMonth() + 1}-${date.getDate() < 10 ? `0${date.getDate()}` : date.getDate()}`;
    } else {
        return '';
    }
}

export function getYYYYMMDD(date: Date) {
    const mm = date.getMonth() + 1;
    const dd = date.getDate();
    return [date.getFullYear(), (mm > 9 ? '' : '0') + mm, (dd > 9 ? '' : '0') + dd].join('-');
}

export function getStringDateByFormat(
    date: string,
    format: string,
    months = {
        1: 'January',
        2: 'February',
        3: 'March',
        4: 'April',
        5: 'May',
        6: 'June',
        7: 'July',
        8: 'August',
        9: 'September',
        10: 'October',
        11: 'November',
        12: 'December'
    }, weekdays = {1: 'Sun', 2: 'Mon', 3: 'Tues', 4: 'Wednes', 5: 'Thurs', 6: 'Fri', 7: 'Satur'}
) {
    const timestamp = new Date(date);

    if (isNaN(timestamp.getTime())) {
        return date;
    }

    let jsdate: Date;
    const weekdaysFront = [weekdays[7], weekdays[1], weekdays[2], weekdays[3], weekdays[4], weekdays[5], weekdays[6]];
    const monthsFront = [months[1], months[2], months[3], months[4], months[5], months[6], months[7], months[8], months[9], months[10], months[11], months[12]];
    format = format.replace(new RegExp('%', 'g'), '');
    const txtWords = weekdaysFront.concat(monthsFront);
    const formatChr = /\\?(.?)/gi;

    const f: {[index: string]: any} = {
        d: function () {
            return _pad(f.j(), 2);
        },
        b: function () {
            return f.F().slice(0, 3);
        },
        D: function () {
            return f.l().slice(0, 3);
        },
        j: function () {
            return jsdate.getDate();
        },
        l: function () {
            return txtWords[f.w()] + 'day';
        },
        N: function () {
            return f.w() || 7;
        },
        S: function () {
            const j = f.j();
            let i = j % 10;
            if (i <= 3 && parseInt(String((j % 100) / 10), 10) === 1) {
                i = 0;
            }
            return ['st', 'nd', 'rd'][i - 1] || 'th';
        },
        w: function () {
            return jsdate.getDay();
        },
        z: function () {
            const a = new Date(f.Y(), f.n() - 1, f.j());
            const b = new Date(f.Y(), 0, 1);
            return Math.round((a.getTime() - b.getTime()) / 864e5);
        },
        W: function () {
            const a = new Date(f.Y(), f.n() - 1, f.j() - f.N() + 3);
            const b = new Date(a.getFullYear(), 0, 4);
            return _pad(1 + Math.round((a.getTime() - b.getTime()) / 864e5 / 7), 2);
        },
        F: function () {
            return txtWords[6 + f.n()];
        },
        m: function () {
            return _pad(f.n(), 2);
        },
        M: function () {
            return f.F();
        },
        n: function () {
            return jsdate.getMonth() + 1;
        },
        t: function () {
            return (new Date(f.Y(), f.n(), 0)).getDate();
        },
        L: function () {
            const j = f.Y();
            return j % 4 === 0 && j % 100 !== 0 || j % 400 === 0;
        },
        o: function () {
            const n = f.n();
            const W = Number(f.W());
            const Y = f.Y();
            return Y + (n === 12 && W < 9 ? 1 : n === 1 && W > 9 ? -1 : 0);
        },
        Y: function () {
            return jsdate.getFullYear();
        },
        y: function () {
            return f.Y().toString().slice(-2);
        },
        a: function () {
            return jsdate.getHours() > 11 ? 'pm' : 'am';
        },
        A: function () {
            return f.a().toUpperCase();
        },
        B: function () {
            const H = jsdate.getUTCHours() * 36e2;
            const i = jsdate.getUTCMinutes() * 60;
            const s = jsdate.getUTCSeconds();
            return _pad(Math.floor((H + i + s + 36e2) / 86.4) % 1e3, 3);
        },
        g: function () {
            return f.G() % 12 || 12;
        },
        G: function () {
            return jsdate.getHours();
        },
        h: function () {
            return _pad(f.g(), 2);
        },
        H: function () {
            return _pad(f.G(), 2);
        },
        i: function () {
            return _pad(jsdate.getMinutes(), 2);
        },
        s: function () {
            return _pad(jsdate.getSeconds(), 2);
        },
        u: function () {
            return _pad(jsdate.getMilliseconds() * 1000, 6);
        },
        e: function () {
            const msg = 'Not supported (see source code of date() for timezone on how to add support)';
            throw new Error(msg);
        },
        I: function () {
            const a = new Date(f.Y(), 0);
            const c = Date.UTC(f.Y(), 0);
            const b = new Date(f.Y(), 6);
            const d = Date.UTC(f.Y(), 6);

            return ((a.getTime() - c) !== (b.getTime() - d)) ? 1 : 0;
        },
        O: function () {
            const tzo = jsdate.getTimezoneOffset();
            const a = Math.abs(tzo);
            return (tzo > 0 ? '-' : '+') + _pad(Math.floor(a / 60) * 100 + a % 60, 4);
        },
        P: function () {
            const O = f.O();
            return (O.substr(0, 3) + ':' + O.substr(3, 2));
        },
        T: function () {
            return 'UTC';
        },
        Z: function () {
            return -jsdate.getTimezoneOffset() * 60;
        },
        c: function () {
            return 'Y-m-d\\TH:i:sP'.replace(formatChr, formatChrCb);
        },
        r: function () {
            return 'D, d M Y H:i:s O'.replace(formatChr, formatChrCb);
        },
        U: function () {
            return jsdate.getDate() / 1000 | 0;
        }
    };

    const _pad = function (n: number | string, c: number) {
        n = String(n);
        while (n.length < c) {
            n = '0' + n;
        }
        return n;
    };

    const formatChrCb = function (type: string, s: string) {
        return f[type] ? f[type]() : s;
    };

    const _date = function (format: string, timestamp: number | Date) {
        jsdate = (timestamp === undefined ? new Date()
            : (timestamp instanceof Date) ? new Date(timestamp)
                : new Date(timestamp * 1000) // UNIX timestamp (auto-convert to int)
        );
        return format.replace(formatChr, formatChrCb);
    };

    return _date(format, timestamp);
}