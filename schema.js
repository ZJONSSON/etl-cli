const etl = require('etl');

function traverseAndCoerce(obj) {
  if (obj !== null && typeof obj === 'object') {
    Object.keys(obj).forEach(key => {
      if (obj[key] !== null && typeof obj[key] === 'object') {
        traverseAndCoerce(obj[key]);
      } else if (!!obj[key] && !Number.isNaN(+obj[key])) {
        obj[key] = +obj[key];
      }
    });
  }
}

function traverseAnyOf(schema) {
  if (schema !== null && typeof schema === 'object') {
    // Check if the current schema has 'anyOf'
    if (Array.isArray(schema.anyOf)) {
      delete schema.anyOf;
      schema.type = 'string';
    }

    // Recursively check all keys of the object
    Object.keys(schema).forEach(key => {
      if (schema[key] !== null && typeof schema[key] === 'object') {
        traverseAnyOf(schema[key]);
      }
    });
  }
  return schema;
}

module.exports.inferSchema = d => {
  const { inferSchema } = require('@jsonhero/schema-infer');
  d.forEach(traverseAndCoerce);
  const schema = inferSchema(d);
  // @ts-ignore
  return traverseAnyOf(schema.toJSONSchema().items);
};

function traverseGlue(d) {
  if (d.items) {
    return `array<${traverseGlue(d.items)}>`;
  }
  if (d.properties) {
    const inner = Object.keys(d.properties).map(key => {
      // console.log(key,top)
      const prefix = `${key}:`;
      const value = traverseGlue(d.properties[key]);
      if (value !== undefined) return `${prefix}${value}`;
    }).filter(d => d).join(',');
    return `struct<${inner}>`;
  }

  if (d.type == 'boolean') return 'boolean';
  if (d.type == 'string') return 'string';
  if (d.type == 'number') return 'double';
  if (d.type == 'integer') {
    if (d.comment == 'INT32' || d.comment == 'UINT32') return 'int';
    return 'bigint';
  }
}

module.exports.glueSchema = schema => {
  const ret = Object.keys(schema.properties).map( (Name) => {
    const Type = traverseGlue(schema.properties[Name]);
    if (Type !== undefined) return { Name, Type };
  }).filter(d => d);
  return etl.toStream([ret]);
};