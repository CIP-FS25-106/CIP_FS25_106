// Add smooth scrolling behavior when navigation links are clicked
window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        smooth_scroll: function(overview_clicks, stations_clicks, temporal_clicks, categories_clicks) {
            const ctx = window.dash_clientside.callback_context;
            
            if (!ctx || !ctx.triggered) {
                return window.dash_clientside.no_update;
            }
            
            const triggered_id = ctx.triggered[0].prop_id.split('.')[0];
            let target_id;
            
            if (triggered_id === 'nav-overview') {
                target_id = 'overview';
            } else if (triggered_id === 'nav-stations') {
                target_id = 'stations';
            } else if (triggered_id === 'nav-temporal') {
                target_id = 'temporal';
            } else if (triggered_id === 'nav-categories') {
                target_id = 'categories';
            } else {
                return window.dash_clientside.no_update;
            }
            
            const target = document.getElementById(target_id);
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
            
            return window.dash_clientside.no_update;
        }
    }
});