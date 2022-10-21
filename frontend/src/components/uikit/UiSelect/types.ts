type UiSelectItem<T> = {
  __type: 'item';
  value: T;
  selected?: boolean;
  disabled?: boolean;
}

type UiSelectGroup<T> = {
  __type: 'group';
  items: UiSelectItemInterface<T, 'item'>[];
}

export type UiSelectItemInterface<T = unknown, V extends 'item' | 'group' = 'item' | 'group'> = {
    id: string | number | symbol;
    label: string;
} & (
  V extends 'item'
    ? UiSelectItem<T>
    : V extends 'group'
      ? UiSelectGroup<T>
      : UiSelectItem<T> | UiSelectGroup<T>
  );
