// Modified JSON parser to parse Athena output
// * Name separator changed from : to =
// * Quotation mark requirement for strings removed
// * Illegal characters in a string now include =,]}
// Source of original JSON parser: https://github.com/pegjs/pegjs/blob/master/examples/json.pegjs

JSON_text
  = ws value:value ws { return value; }

begin_array     = ws "[" ws
begin_object    = ws "{" ws
end_array       = ws "]" ws
end_object      = ws "}" ws
name_separator  = ws "=" ws
value_separator = ws "," ws

ws "whitespace" = [ \t\n\r]*

value
  = false
  / null
  / true
  / object
  / array
  / string

false = "false" { return false; }
null  = "null"  { return null;  }
true  = "true"  { return true;  }

object
  = begin_object
    members:(
      head:member
      tail:(value_separator m:member { return m; })*
      {
        var result = {};
        var arr = [head].concat(tail);
        var lastIndex = -1;
        arr.forEach(function(element,index) {
          if (!element.name) {
            result[arr[lastIndex].name] += ', ' + element.value;
          }
          else {
            lastIndex = index;
            result[element.name] = element.value;
          }
        });

        return result;
      }
    )?
    end_object
    { return members !== null ? members: {}; }

member
  = name:string name_separator value:value {
      return { name: name, value: value };
    }
    / value:value {
      return {value:value};
    }

array
  = begin_array
    values:(
      head:value
      tail:(value_separator v:value { return v; })*
      { return [head].concat(tail); }
    )?
    end_array
    { return values !== null ? values : []; }


string "string"
  = chars:char*  { 
  let s = chars.join(""); 
    return isNaN(s)? s : +s;
 }

char
 = [^"=,}\]]
  / ["] { return ''}