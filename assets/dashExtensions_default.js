window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, context) {
            const {
                classes,
                colorscale,
                style,
                colorProp
            } = context.props.hideout; // get props from hideout
            const value = feature.properties[colorProp]; // get the value that determines the color

            // Loop through classes and assign colors from colorscale
            for (let i = 0; i < classes.length; ++i) {
                if (value === classes[i]) { // Check for exact match with the city
                    style.fillColor = colorscale[i]; // Set fill color based on the city's ratio
                    break;
                }
            }
            return style; // Return the modified style
        }
    }
});