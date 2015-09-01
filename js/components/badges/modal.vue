<style lang="less">

</style>

<template>
<modal title="{{ _('Badges') }}"
    class="modal-info badges-modal"
    v-ref="modal">

    <div class="modal-body">
        <div class="text-center row">
            <p class="lead col-xs-12" v-if="hasBadges">{{ _('Pick the active badges') }}</p>
            <p class="lead col-xs-12" v-if="!hasBadges">{{ _('No badge available') }}</p>
        </div>
        <div class="text-center row">
            <div class="badges col-xs-6 col-xs-offset-3">
                <button class="btn btn-primary btn-flat btn-block"
                    v-repeat="badges"
                    v-on="click: toggle($key)"
                    v-class="active: selected.indexOf($key) >= 0">
                    <span class="fa pull-left" v-class="
                        fa-bookmark: selected.indexOf($key) >= 0,
                        fa-bookmark-o: selected.indexOf($key) < 0
                        "></span>
                    {{ $value }}
                </button>
            </div>
        </div>
    </div>

    <footer class="modal-footer text-center">
        <button v-attr="disabled: !hasModifications" type="button"
            class="btn btn-success btn-flat pointer pull-left"
            v-on="click: confirm">
            {{ _('Confirm') }}
        </button>
        <button v-if="confirm" type="button" class="btn btn-danger btn-flat pointer"
            data-dismiss="modal">
            {{ _('Cancel') }}
        </button>
    </footer>
</modal>
</template>

<script>
'use strict';

var API = require('api'),
    badges = require('models/badges').badges;

module.exports = {
    name: 'BadgesModal',
    components: {
        'modal': require('components/modal.vue')
    },
    data: function() {
        return {
            selected: [],
            initial: [],
            subject: null,
            badges: {},
            added: {},
            removed: {}
        };
    },
    computed: {
        basename: function() {
            return this.subject.__class__.toLowerCase();
        },
        namespace: function() {
            return this.basename + 's';
        },
        hasBadges: function() {
            return Object.keys(this.badges).length > 0;
        },
        hasModifications: function() {
            return (this.selected.length !== this.initial.length)
                || this.selected.some(function(badge) {
                    return this.initial.indexOf(badge) < 0;
                }.bind(this));
        }
    },
    compiled: function() {
        this.badges = badges[this.basename];

        if (this.subject.hasOwnProperty('badges')) {
            this.selected = this.subject.badges.map(function(badge) {
                return badge.kind;
            });

            this.initial = this.selected.slice(0);
        }
    },
    methods: {
        confirm: function() {
            var add_operation = 'add_' + this.basename + '_badge',
                remove_operation = 'delete_' + this.basename + '_badge',
                to_add = this.selected.filter(function(badge) {
                        return this.initial.indexOf(badge) < 0;
                    }.bind(this)),
                to_remove = this.initial.filter(function(badge) {
                        return this.selected.indexOf(badge) < 0;
                    }.bind(this));


            to_add.forEach(function(badge) {
                var data = {payload: {kind: badge}},
                    key = this.basename === 'organization' ? 'org' :this.basename;

                data[key] = this.subject.id;
                this.added[badge] = false;

                API[this.namespace][add_operation](data, function(response) {
                    this.subject.badges.push(response.obj);
                    this.added[badge] = true;
                    this.checkAllDone();
                }.bind(this));
            }.bind(this));

            to_remove.forEach(function(badge) {
                var data = {badge_kind: badge},
                    key = this.basename === 'organization' ? 'org' : this.basename;

                data[key] = this.subject.id;
                this.removed[badge] = false;

                API[this.namespace][remove_operation](data, function(response) {
                    var badgeObj = this.subject.badges.filter(function(o) {
                            return o.kind === badge;
                        })[0];
                    this.subject.badges.$remove(badgeObj);
                    this.removed[badge] = true;
                    this.checkAllDone();
                }.bind(this));
            }.bind(this));
        },
        checkAllDone: function() {
            var allAdded = Object.keys(this.added).every(function(key) {
                    return this.added[key];
                }.bind(this)),
                allRemoved = Object.keys(this.removed).every(function(key) {
                    return this.removed[key];
                }.bind(this));

            if (allAdded && allRemoved) {
                this.$.modal.close();
                this.$emit('badges:modified');
            }
        },
        toggle: function(badge) {
            if (this.selected.indexOf(badge) >= 0) {
                this.selected.$remove(badge);
            } else {
                this.selected.push(badge);
            }
        }
    }
};
</script>