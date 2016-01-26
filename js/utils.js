/**
 * Check if an object is a function
 */
export function isFunction(obj) {
    return obj && Object.prototype.toString.call(obj) === '[object Function]';
}

/**
 * Check if an object is an Object
 */
export function isObject(obj) {
    return obj === Object(obj);
}

/**
 * Check if an object is a String
 */
export function isString(obj) {
    return typeof obj === 'string' || obj instanceof String;
}

/**
 * A property getter resolving dot-notation
 * @param  {Object} obj  The root object to fetch property on
 * @param  {String} name The optionnaly dotted property name to fetch
 * @return {Object}      The resolved property value
 */
export function getattr(obj, name) {
    if (!obj || !name) return;
    const names = name.split('.');
    while(names.length && (obj = obj[names.shift()]));
    return obj;
}

/**
 * A property setter resolving dot-notation
 * @param  {Object} obj   The root object to set property on
 * @param  {String} name  The optionnaly dotted property name to set
 * @param  {Object} value The value to set
 */
export function setattr(obj, name, value) {
    if (!obj || !name) return;
    const names = name.split('.');
    while (names.length && (name = names.shift()) && names.length) {
        if (!obj.hasOwnProperty(name)) obj[name] = {};
        obj = obj[name];
    }
    obj[name] = value;
}


export default {
    isFunction,
    isObject,
    isString,
    getattr,
    setattr
};
